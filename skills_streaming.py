"""
Skill-aware streaming for the universal agent.

Two pieces:
  1. agent.py      -> `stream_universal_skill_turn_events(...)`  (event normaliser)
  2. bot service   -> `invoke_universal_skills_streaming_bot(...)` (SSE endpoint)

Wire contract (mirrors the sub-agent endpoint):
  reasoning              live `step`, carries `skill` label (skill loaded / tool used / tool done)
  skill_stream_chunk     intermediate model text produced while working a skill (carries `skill`)
  stream_start/chunk/end final answer bracket, all pinned to one `text_step`
  response_file          final incremented step, `sql` JSON-encoded, plus `skills_used`

Assumption: skills are deepagents SkillsMiddleware skills, i.e. the model "activates" a skill by
calling `read_file` on `<skills_dir>/<skill_name>/SKILL.md`. Adjust SKILL_MD_RE / SKILL_READ_TOOLS
if your skills are exposed differently (e.g. one tool per skill).
"""

# =============================================================================================
# 1) agent.py
# =============================================================================================
import json
import re
import time
import uuid

from langchain_core.messages import HumanMessage

SKILL_MD_RE = re.compile(r"(?:^|/)skills/(?P<name>[^/]+)/SKILL\.md$", re.IGNORECASE)
SKILL_READ_TOOLS = {"read_file"}

# Unlike sub-agents, skill work and the final answer come from the SAME model, so we classify
# each model run: a run that emits tool calls is intermediate (skill work); a run that ends
# with no tool calls is the final answer. Text is buffered until we can tell. If a run's text
# grows past this many chars with no tool call yet, we treat it as the final answer and start
# streaming it live (pre-tool "Let me check..." preambles are far shorter than this).
PENDING_TEXT_LIMIT = 240


def _chunk_text(chunk) -> str:
    """Model chunks may carry a str or a list of content blocks (Anthropic-style)."""
    content = getattr(chunk, "content", "") if chunk is not None else ""
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        return "".join(
            b.get("text", "") for b in content
            if isinstance(b, dict) and b.get("type") in ("text", "text_delta")
        )
    return ""


def _is_nested(event) -> bool:
    """True for events from inside a delegated sub-agent (task tool) rather than the coordinator."""
    ns = (event.get("metadata") or {}).get("langgraph_checkpoint_ns", "") or ""
    return "tools:" in ns


def _skill_from_tool_input(tool_input) -> str | None:
    if not isinstance(tool_input, dict):
        return None
    path = tool_input.get("file_path") or tool_input.get("path") or ""
    match = SKILL_MD_RE.search(str(path))
    return match.group("name") if match else None


def _short(value, limit=160) -> str:
    text = value if isinstance(value, str) else json.dumps(value, default=str)
    return text if len(text) <= limit else text[: limit - 1] + "…"


async def stream_universal_skill_turn_events(user_input, user_id, chat_id, context):
    """Yield normalised events:
        on_skill_start              {'skill'}
        on_skill_progress           {'skill', 'kind', 'detail'}
        on_skill_chat_model_stream  {'skill', 'data': {'chunk': text}}
        on_chat_model_stream        {'data': {'chunk': text}}       (final answer)
        on_chain_end                name == 'LangGraph'             (final state)
    """
    # Swap in however `invoke_universal_agent_full` builds the skills-enabled agent + config.
    agent = build_universal_skill_agent(context)  # noqa: F821 - your factory
    config = {"configurable": {"thread_id": chat_id, "user_id": user_id}}
    inputs = {"messages": [HumanMessage(content=user_input)]}

    active_skill = None
    runs = {}  # run_id -> {"mode": "pending" | "skill" | "final", "buf": str}

    def skill_text(text):
        return {"event": "on_skill_chat_model_stream", "skill": active_skill or "coordinator",
                "data": {"chunk": text}}

    def final_text(text):
        return {"event": "on_chat_model_stream", "data": {"chunk": text}}

    async for ev in agent.astream_events(inputs, config=config, version="v2"):
        kind = ev.get("event")
        run_id = ev.get("run_id")
        data = ev.get("data") or {}

        if kind == "on_chain_end" and ev.get("name") == "LangGraph" and not ev.get("parent_ids"):
            yield ev
            continue

        if _is_nested(ev):
            continue

        if kind == "on_chat_model_start":
            runs[run_id] = {"mode": "pending", "buf": ""}

        elif kind == "on_chat_model_stream":
            state = runs.setdefault(run_id, {"mode": "pending", "buf": ""})
            chunk = data.get("chunk")
            text = _chunk_text(chunk)
            has_tool_call = bool(getattr(chunk, "tool_call_chunks", None))

            if state["mode"] == "pending" and has_tool_call:
                state["mode"] = "skill"
                if state["buf"]:
                    yield skill_text(state["buf"])
                    state["buf"] = ""

            if not text:
                continue
            if state["mode"] == "final":
                yield final_text(text)
            elif state["mode"] == "skill":
                yield skill_text(text)
            else:
                state["buf"] += text
                if len(state["buf"]) > PENDING_TEXT_LIMIT:
                    state["mode"] = "final"
                    yield final_text(state["buf"])
                    state["buf"] = ""

        elif kind == "on_chat_model_end":
            state = runs.pop(run_id, None)
            if state and state["mode"] == "pending" and state["buf"]:
                output = data.get("output")
                if getattr(output, "tool_calls", None):
                    yield skill_text(state["buf"])
                else:
                    yield final_text(state["buf"])

        elif kind == "on_tool_start":
            tool_name = ev.get("name", "")
            tool_input = data.get("input")
            skill = _skill_from_tool_input(tool_input) if tool_name in SKILL_READ_TOOLS else None
            if skill:
                active_skill = skill
                yield {"event": "on_skill_start", "skill": skill}
            elif active_skill:
                yield {"event": "on_skill_progress", "skill": active_skill, "kind": "tool_start",
                       "detail": f"{tool_name}: {_short(tool_input)}"}

        elif kind == "on_tool_end":
            tool_name = ev.get("name", "")
            if active_skill and tool_name not in SKILL_READ_TOOLS:
                yield {"event": "on_skill_progress", "skill": active_skill, "kind": "tool_end",
                       "detail": tool_name}


# =============================================================================================
# 2) bot service  (method on the same class as invoke_universal_streaming_bot)
# =============================================================================================
def _render_skill_progress(event) -> str:
    skill = event.get("skill", "unknown")
    if event.get("event") == "on_skill_start":
        return f"Using skill: {skill}"
    if event.get("kind") == "tool_start":
        return f"[{skill}] Running {event.get('detail', '')}"
    if event.get("kind") == "tool_end":
        return f"[{skill}] Finished {event.get('detail', '')}"
    return ""


async def invoke_universal_skills_streaming_bot(self, payload):
    """SSE stream for the skills-based universal agent. Same contract as
    `invoke_universal_streaming_bot`, with `skill_stream_chunk` in place of
    `subagent_stream_chunk` and a `skill` label on reasoning / intermediate events."""
    output = ""
    sql, intent = [], []
    reasoning_text = ""
    step = 0
    text_step = None
    result = None
    is_streaming = False
    skills_used = []
    try:
        start_time = time.perf_counter()
        message_id = str(uuid.uuid4())
        logger.info(  # noqa: F821
            f"[invoke_universal_skills_streaming_bot] Starting chat_id={payload.chat_id}, "
            f"user_id={payload.user_id}, user_input={payload.user_input!r}"
        )
        context = UniversalAgentContext(user_id=payload.user_id, chat_id=payload.chat_id)  # noqa: F821

        def sse(obj, **dump_kwargs):
            return f"data: {json.dumps(obj, **dump_kwargs)} \n\n"

        def base(**extra):
            return {"chat_id": payload.chat_id, "user_id": payload.user_id,
                    "message_id": message_id, **extra}

        async for event in stream_universal_skill_turn_events(
            payload.user_input, payload.user_id, payload.chat_id, context,
        ):
            event_type = event.get("event")

            if event_type in ("on_skill_start", "on_skill_progress"):
                if event_type == "on_skill_start" and event["skill"] not in skills_used:
                    skills_used.append(event["skill"])
                content = _render_skill_progress(event)
                if content:
                    step += 1
                    yield sse(base(content=content, type="reasoning", step=step,
                                   skill=event.get("skill", "unknown")))
                continue

            if event_type == "on_skill_chat_model_stream":
                chunk = (event.get("data") or {}).get("chunk") or ""
                if chunk:
                    yield sse(base(content=chunk, type="skill_stream_chunk", step=step,
                                   skill=event.get("skill", "unknown")))
                continue

            if event_type == "on_chat_model_stream":
                chunk = (event.get("data") or {}).get("chunk") or ""
                if chunk:
                    if not is_streaming:
                        is_streaming = True
                        text_step = step
                        yield sse(base(content="", type="stream_start", step=text_step))
                    yield sse(base(content=chunk, type="stream_chunk", step=text_step))
                continue

            if event_type == "on_chain_end" and event.get("name") == "LangGraph":
                result = (event.get("data") or {}).get("output") or {}

        if result is None:
            raise RuntimeError("Universal skills agent completed without a LangGraph output event")

        full_result = build_full_result(  # noqa: F821
            payload.user_input, payload.user_id, payload.chat_id, result, context
        )
        output = self.sanitize_unicode(full_result["model_output"])
        sql = full_result["sql"]
        intent = full_result["intent"]
        reasoning_text = full_result["reasoning"]

        if not is_streaming:
            text_step = step
            yield sse(base(content="", type="stream_start", step=text_step))
            yield sse(base(content=output, type="stream_chunk", step=text_step))
        yield sse(base(content="", type="stream_end", step=text_step))

        step += 1
        combined_results = universal_sql_data_source_to_records(full_result["sql_data_source"])  # noqa: F821
        yield sse(base(
            type="response_file", step=step, sql=json.dumps(sql),
            result_dataframe=json.dumps(combined_results),
            sql_data_source=full_result["sql_data_source"],
            general_statement=reasoning_text, sql_tables=full_result["sql_tables"],
            skills_used=skills_used,
        ), ensure_ascii=False)

        execution_time = time.perf_counter() - start_time
        final_output = {"model_output": output, "sql": sql, "intent": intent,
                        "reasoning": reasoning_text, "skills_used": skills_used}
        sql_service.insert_session_data({  # noqa: F821
            "message_id": message_id, "chat_id": payload.chat_id, "user_id": payload.user_id,
            "input_message": json.dumps({"user_input": payload.user_input,
                                         "formatted_input": payload.user_input}),
            "model_response": json.dumps(final_output), "vote": "", "feedback": "",
            "latency": int(execution_time), "topic_id": "universal_skills_agent",
            "meta_data": json.dumps({"skills_used": skills_used}),
        })
        logger.info(  # noqa: F821
            f"[invoke_universal_skills_streaming_bot] Completed chat_id={payload.chat_id}, "
            f"latency={execution_time:.2f}s, skills={skills_used}, output_length={len(output)}"
        )

    except Exception as e:
        yield _format_stream_error_event(  # noqa: F821
            e, step, payload.chat_id, payload.user_id, locals().get("message_id", ""),
        )

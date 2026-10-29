"""
Multiple-product lookup pipeline.

Reads an uploaded CSV/Excel file containing product identifiers (Product Name,
SP Number, OPN), queries the product and BOM tables for matching records,
groups and cleans the results (location codes/names, LOCK/M9 release-date
rules, invalid identifiers), sorts them in the order the user supplied, and
exports the final result as an Excel file (bytes).
"""
import copy
import os.path
import pandas as pd
import sys
import re
import traceback
import logging
from pathlib import Path
sys.path.append(str(Path(__file__).resolve().parent.parent))
from configs.config_data import configData
from sql_services import sqlServices
from graphs.merge_files import ExcelExporter

logger = logging.getLogger(__name__)

config = configData()
sql_service = sqlServices()
export = ExcelExporter()
column_mapping = {
    "SALES_NAME": "Product Name",
    "SP": "SP Number",
    "OPN": "OPN",
    "HFGR": "HFGRP",
    "DIV" : "DIV",
    "PL":"PL",
    "Quality_Req_Category":"Quality Req Category",
    "Pre_M9": "Pre_M9",
    "M9_Release_Date": "M9_Release_Date",
    "Material_Status": "Material_Status",
    "Product_Status": "Product_Status",
    "ESD_Classification_HBM": "ESD Data HBM",
    "ESD_Classification_CDM": "ESD Data CDM",
    "Package_Name": "Product Package",
    "Packaging_Type": "Product Packing Type",
    "Small_Packing_Unit": "Small Packing Unit",
    "Large_Packing_Unit": "Large Packing Unit",
    "Wafer_Fab_Loc": "Wafer_Fab_Loc",
    "Wafer_Fab_Loc_Name": "Wafer_Fab_Loc_Name",
    "Wafer_Test_Loc": "Wafer_Test_Loc",
    "Wafer_Test_Loc_Name": "Wafer_Test_Loc_Name",
    "Assembly_Loc": "Assembly_Loc",
    "Assembly_Loc_Name": "Assembly_Loc_Name",
    "Final_Test_Loc": "Final_Test_Loc",
    "Final_Test_Loc_Name": "Final_Test_Loc_Name",
    "Halogen_Free": "Halogen Free",
    "Completely_Lead_Free": "Completely Lead Free",
    "ROHS_Compliance": "ROHS Compliance",
    "raster_x": "Die Raster X",
    "raster_y": "Die Raster Y",
    "die_thickness": "Die Thickness",
    "waferdiameter": "Wafer Diameter",
    "process_line_min_feat_size": "Process Node",
    "Process_Technology": "Product Technology",
    "ProcessGroup" : "Process Group",
    "Moisture_Level":"MSL",
    "Moisture_Packing": "Moisture Packing",
    "Max_Storage_Days": "Max Storage days",
    "be_temperature": "Wafer Test Temperature",
    "fe_temperature": "Final Test Temperature",
    "body_length": "Body Length",
    "body_width": "Body Width",
    "body_thickness": "Body Thickness",
    "net_weight": "Net Weight",
    "moulding_compound": "Moulding Compound",
    "leadframe_material": "Leadframe",
    "plating_final_surface": "Plating Surface",
    "mmpa_url": "Marking Format",
    "mtmc_mmpa_pic_no": "PIC No (Marking Pattern No)",
    "Quality_Milestone":"Quality_Milestone",
}

product_keys = ["SALES_NAME", "SP", "OPN", "HFGR", "DIV", "PL", "Quality_Req_Category", "Pre_M9", "M9_Release_Date", "Material_Status", "Product_Status", "ESD_Classification_HBM", "ESD_Classification_CDM", "Package_Name", "Packaging_Type", "Small_Packing_Unit", "Large_Packing_Unit", "Wafer_Fab_Loc", "Wafer_Fab_Loc_Name", "Wafer_Test_Loc", "Wafer_Test_Loc_Name", "Assembly_Loc", "Assembly_Loc_Name", "Final_Test_Loc", "Final_Test_Loc_Name", "Halogen_Free", "Completely_Lead_Free", "ROHS_Compliance", "Moisture_Level", "Moisture_Packing", "Max_Storage_Days", "Quality_Milestone", "Process_Technology", "ProcessGroup"]
bom_keys = ["body_length", "body_thickness", "body_width", "waferdiameter", "raster_x", "raster_y", "process_line_min_feat_size", "moulding_compound", "leadframe_material", "plating_final_surface", "net_weight", "be_temperature", "fe_temperature", "mtmc_mmpa_pic_no", "mmpa_url", "die_thickness"]
column_pairs = {
            'Wafer_Fab_Loc': 'Wafer_Fab_Loc_Name',
            'Wafer_Test_Loc': 'Wafer_Test_Loc_Name',
            'Assembly_Loc': 'Assembly_Loc_Name',
            'Final_Test_Loc': 'Final_Test_Loc_Name'
        }
class multipleProduct(object):
    """
    Handles bulk product lookups from an uploaded file.

    The main entry point is :meth:`process_multiple_product`, which reads the
    input file, queries the database for every identifier (SP Number, OPN or
    Product Name), applies the business rules for location grouping,
    LOCK / M9 release dates and invalid identifiers, and returns the final
    Excel output in ``input_dict['final_answer']``.
    """

    def read_input_file(self, input_dict):
        """
        Read the user's uploaded CSV or Excel file.

        For Excel files the header is taken from the second row (``header=1``)
        and column names are stripped of surrounding whitespace.

        Args:
            input_dict (dict): Request payload; must contain ``'filepath'``.

        Returns:
            tuple[pd.DataFrame, list]: The loaded dataframe and its list of
            column names. Both are empty if the file type is unsupported or
            reading fails.
        """
        dataframe = pd.DataFrame()
        columns = []
        try:

            filepath = input_dict['filepath']
            file_extension = filepath.lower().split('.')[-1]
            if file_extension == 'csv':
                dataframe = pd.read_csv(filepath)
                columns = dataframe.columns.tolist()

            elif file_extension in ['xlsx', 'xls']:
                dataframe = pd.read_excel(filepath, header=1)
                columns = [str(col).strip() for col in dataframe.columns.tolist()]

        except Exception:
            print(f'Error in read_input_file {traceback.print_exc()}')
        return dataframe, columns

    def build_like_conditions(self, field, items):
        """
        Build an OR-joined set of SQL ``LIKE '%item%'`` conditions for a field.

        Args:
            field (str): Fully qualified column name, e.g. ``'q.SP'``.
            items (list): Values to match partially.

        Returns:
            str: A parenthesised SQL condition string, e.g.
            ``"(q.SP LIKE '%A%' OR q.SP LIKE '%B%')"``.
        """
        conditions = " OR ".join(f"{field} LIKE '%{item}%'" for item in items)
        return f"({conditions})"

    def generate_sql_query(self, product_list, formation_type):
        """
        Generate the SQL query that fetches product and BOM data.

        The product table (``QUAI_product_processed``) is left-joined to the
        configured BOM table on sales name, basic type and MA/FP number.

        Args:
            product_list (list): Identifiers to search for (SP Number, OPN or
                Product Name).
            formation_type (str): ``"exact"`` uses ``IN (...)`` matching and
                selects only the columns in ``product_keys``/``bom_keys``;
                any other value uses partial ``LIKE`` matching and selects all
                columns.

        Returns:
            str: The SQL query string, or ``""`` if generation fails.
        """
        query = ""
        try:
            q_select = ", ".join(f"q.[{c}]" for c in product_keys)
            b_select = ", ".join(f"b.[{c}]" for c in bom_keys)
            if formation_type == "exact":
                sql_format = ", ".join(f"'{item}'" for item in product_list)
                query = f"""
                                       SELECT DISTINCT {q_select}, {b_select}
                                       FROM
                                           [dbo].[QUAI_product_processed] q
                                       LEFT JOIN
                                           [dbo].[{config.bom_table}] b
                                               ON q.SALES_NAME = b.sales_product_name
                                               AND q.Basic_Type = b.basic_type
                                               AND q.MA         = b.fp_no
                                       WHERE
                                           q.SP            IN ({sql_format})
                                           OR q.OPN        IN ({sql_format})
                                           OR q.SALES_NAME IN ({sql_format})
                                    """
            else:
                query = f"""
                    SELECT q.*, b.*
                    FROM
                        [dbo].[QUAI_product_processed] q
                    LEFT JOIN
                        [dbo].[{config.bom_table}]  b
                            ON q.SALES_NAME = b.sales_product_name
                            AND q.Basic_Type = b.basic_type
                            AND q.MA         = b.fp_no
                    WHERE
                        {self.build_like_conditions('q.SP', product_list)}
                        OR {self.build_like_conditions('q.OPN', product_list)}
                        OR {self.build_like_conditions('q.SALES_NAME', product_list)}
                """

        except Exception:
            print(f'Error in text_to_sql_agent {traceback.print_exc()}')
        return query

    # def join_unique(self, s):
    #     seen = []
    #     seen_set = set()
    #     try:
    #         for v in s:
    #             if pd.isna(v):
    #                 continue
    #             v = str(v).strip()
    #             if v == "" or v.lower() in ("none", "nan", "null"):
    #                 continue
    #             if v not in seen_set:
    #                 seen_set.add(v)
    #                 seen.append(v)
    #     except Exception:
    #         print(f'Error in join_unique {traceback.print_exc()}')
    #     return ','.join(seen)

    # def join_unique(self, series):
    #     # dict.fromkeys preserves first-seen order while dropping dupes
    #     vals = [v for v in series if v != '']
    #     return ','.join(dict.fromkeys(vals))

    def clean_cell(self, val):
        """
        Deduplicate and naturally sort the values inside a single cell.

        The cell is split on ``','`` and ``'\\n'``; duplicates are removed
        case-insensitively (first occurrence kept), the remaining values are
        sorted alphanumerically (natural sort, so ``"A2"`` comes before
        ``"A10"``), and the string is rebuilt reusing the original separators
        positionally (falling back to ``','``), each followed by a space.

        Args:
            val: Cell value; NaN values are returned unchanged.

        Returns:
            str | float: The cleaned string, or the original NaN.
        """
        if pd.isna(val):
            return val

        existing_str = str(val).strip()

        # Split by ',' and '\n' but preserve separators
        parts = re.split(r'([,\n])', existing_str)

        # Extract values and separators
        values = [v.strip() for v in parts[0::2]]  # Values: index 0, 2, 4, ...
        separators = parts[1::2]  # Separators: index 1, 3, 5, ...

        # Remove duplicates (case-insensitive) keeping original order
        seen = set()
        unique_values = []
        unique_separators = []

        for i, v in enumerate(values):
            if v and v.lower() not in seen:
                seen.add(v.lower())
                unique_values.append(v)
                if i < len(separators):
                    unique_separators.append(separators[i])

        # Sort unique values alphanumerically
        sorted_values = sorted(
            unique_values,
            key=lambda x: [
                int(c) if c.isdigit() else c.lower()
                for c in re.split(r'(\d+)', x)
            ]
        )

        # Rebuild result using original separators positionally
        result = ''
        for i, value in enumerate(sorted_values):
            result += value
            if i < len(sorted_values) - 1:
                sep = unique_separators[i] if i < len(unique_separators) else ','
                result += sep + ' '

        return result

    def remove_duplicates_from_joined(self, values):
        """
        Merge ``';'``-joined strings from a Series into one deduplicated string.

        Each unique non-null value is split on ``';'``, parts are stripped, and
        exact-duplicate parts are dropped while preserving first-seen order.

        Args:
            values (pd.Series): Series of strings that may contain
                ``';'``-separated parts.

        Returns:
            str: The unique parts re-joined with ``'; '``.
        """
        # Split each value by '; ' and collect all parts
        all_parts = []
        seen = set()

        for val in values.dropna().astype(str).unique():
            parts = [p.strip() for p in val.split(';')]
            for part in parts:
                if part not in seen:
                    seen.add(part)
                    all_parts.append(part)

        # Rejoin with '; ' and return as single string
        combined = '; '.join(all_parts)
        return combined

    def smart_map(self, value, mapping):
        """
        Map a location code (or a list of codes) to its location name(s).

        1. Try an exact match of the whole value in ``mapping``.
        2. Otherwise split the value on ``','`` or ``'\\n'`` and map each part
           individually; parts without a valid mapping are kept as-is.
        3. Return ``'Unknown'`` for empty/NaN input or when nothing remains.

        Mapped values that are ``None``/NaN are treated as missing so the final
        join never receives a non-string item.

        Args:
            value: Location code(s) from the grouped dataframe.
            mapping (dict): Code-to-name lookup built from the query result.

        Returns:
            str: The mapped name, the mapped names joined with ``'\\n '``, or
            ``'Unknown'``.
        """
        if pd.isna(value) or str(value).strip() == "":
            return 'Unknown'

        def _valid(v):
            """Return True only if ``v`` is not None and not NaN."""
            # True only if v exists and is not None/NaN
            return v is not None and not (isinstance(v, float) and pd.isna(v))

        # Step 1: Try exact match first
        if value in mapping and _valid(mapping[value]):
            return mapping[value]

        # Step 2: Split by ',' or '\n' and find mapping for each part
        parts = re.split(r'[,\n]', str(value))
        parts = [p.strip() for p in parts if p.strip() != ""]  # remove extra spaces + empty tokens

        if not parts:
            return 'Unknown'

        mapped_parts = []
        for part in parts:
            mapped_value = mapping.get(part)
            if _valid(mapped_value):
                mapped_parts.append(str(mapped_value))
            else:
                mapped_parts.append(part)  # keep original if not found or mapped to None/NaN

        if mapped_parts:
            return '\n '.join(mapped_parts)
        return 'Unknown'

    def group_data(self, df):
        """
        Consolidate location columns per SP Number.

        For each SP Number, all unique Wafer Fab / Wafer Test / Assembly /
        Final Test location codes are joined with newlines, cleaned via
        :meth:`clean_cell`, and their matching location names are rebuilt via
        :meth:`smart_map`. The consolidated values are written back onto every
        row of that SP Number, and duplicate rows are then dropped.

        Args:
            df (pd.DataFrame): Query result with display column names
                (must include ``'SP Number'`` and the location columns).

        Returns:
            pd.DataFrame: Deduplicated dataframe with grouped location
            columns, or an empty dataframe on error.
        """
        result = pd.DataFrame()
        try:
            wafer_fab = dict(zip(df['Wafer_Fab_Loc'], df['Wafer_Fab_Loc_Name']))
            wafer_test = dict(zip(df['Wafer_Test_Loc'], df['Wafer_Test_Loc_Name']))
            assembly_loc = dict(zip(df['Assembly_Loc'], df['Assembly_Loc_Name']))
            final_test =dict(zip(df['Final_Test_Loc'], df['Final_Test_Loc_Name']))
            result_df = df.groupby('SP Number')[list(column_pairs.keys())].agg(
                lambda x: '\n'.join(x.dropna().astype(str).unique())
            ).reset_index()
            result_df[list(column_pairs.keys())] = result_df[list(column_pairs.keys())].apply(lambda col: col.apply(self.clean_cell))
            result_df['Wafer_Fab_Loc_Name'] = result_df['Wafer_Fab_Loc'].apply(lambda x: self.smart_map(x, wafer_fab))
            result_df['Wafer_Test_Loc_Name'] = result_df['Wafer_Test_Loc'].apply(lambda x: self.smart_map(x, wafer_test))
            result_df['Assembly_Loc_Name'] = result_df['Assembly_Loc'].apply(lambda x: self.smart_map(x, assembly_loc))
            result_df['Final_Test_Loc_Name'] = result_df['Final_Test_Loc'].apply(lambda x: self.smart_map(x, final_test))
            df = df.set_index('SP Number')
            result_df = result_df.set_index('SP Number')
            df.update(result_df)
            df = df.reset_index()
            result = df.drop_duplicates()
        except Exception:
            print(f'Error in group_data {traceback.print_exc()}')
        return result

    def handle_undefined_rows_with_lock(self, df, requested_columns):
        """
        Collapse UNDEFINED rows into one per SP Number when a LOCK row exists.

        Only applied when ``'M9_Release_Date'`` is among the requested
        columns. For each SP Number group independently: if the group contains
        a row with ``Material_Status == 'LOCK'``, its ``'UNDEFINED'`` rows have
        ``M9_Release_Date`` cleared and are reduced to a single row, which is
        appended after the group's non-UNDEFINED rows.

        Args:
            df (pd.DataFrame): Dataframe containing ``'SP Number'``,
                ``'Material_Status'`` and ``'M9_Release_Date'`` columns.
            requested_columns (list): Columns requested in the user's file.

        Returns:
            pd.DataFrame: Processed dataframe (or the input unchanged if the
            rule does not apply or an error occurs).
        """
        try:
            if 'M9_Release_Date' not in requested_columns:
                return df

            processed_groups = []

            # Process each unique SP# group independently
            for sp_number, group in df.groupby("SP Number"):
                group = group.copy()

                # Check if LOCK exists WITHIN this specific SP# group
                if 'LOCK' in group['Material_Status'].values:
                    undefined_rows = group[group['Material_Status'] == 'UNDEFINED']

                    if not undefined_rows.empty:
                        undefined_rows = undefined_rows.copy()  # avoid SettingWithCopyWarning
                        undefined_rows['M9_Release_Date'] = ""
                        undefined_rows = undefined_rows.drop_duplicates(subset=['Material_Status']).head(1)

                    # Combine non-UNDEFINED rows + single UNDEFINED row for this SP# group
                    group = pd.concat(
                        [group[group['Material_Status'] != 'UNDEFINED'], undefined_rows],
                        ignore_index=True
                    )

                processed_groups.append(group)

            # Combine all processed SP# groups back into a single dataframe
            df = pd.concat(processed_groups, ignore_index=True)

        except Exception:
            print(f'Error in handle_undefined_rows_with_lock: {traceback.format_exc()}')

        return df

    def preserve_lock_premi9_row(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Keep LOCK rows with Pre-M9 = 'Yes' separate when no UNDEFINED row exists.

        Business rule: if there is a row with ``Material_Status == 'LOCK'`` and
        ``Pre_M9 == 'Yes'``, and no row with ``Material_Status == 'UNDEFINED'``,
        those LOCK rows are kept untouched and moved to the end of the
        dataframe.

        Note: currently not called (its call site in
        :meth:`update_lock_status` is commented out).

        Args:
            df (pd.DataFrame): Dataframe with ``'Material_Status'`` and
                ``'Pre_M9'`` columns.

        Returns:
            pd.DataFrame: Reordered dataframe if the rule applies, otherwise
            the input unchanged.
        """

        # Condition A: Check if LOCK row with Pre-M9 = 'Yes' exists
        try:
            lock_premi9_mask = (df['Material_Status'] == 'LOCK') & (df['Pre_M9'] == 'Yes')
            has_lock_premi9 = lock_premi9_mask.any()

            # Condition B: Check if NO 'Undefined' row exists
            has_undefined = (df['Material_Status'] == 'UNDEFINED').any()

            # Apply rule only if both conditions are met
            if has_lock_premi9 and not has_undefined:
                # Extract the LOCK + Pre-M9=Yes row(s) — kept separately/untouched
                preserved_row = df[lock_premi9_mask].copy()

                # Get all other rows (excluding the preserved LOCK row)
                other_rows = df[~lock_premi9_mask]

                # Combine — preserved row displayed separately, appended at the end (or top, based on preference)
                result_df = pd.concat([other_rows, preserved_row], ignore_index=True)

                return result_df
        except Exception:
            print(f'Error in preserve_lock_premi9_row {traceback.print_exc()}')
        return df

    def update_lock_status(self, grouped_csv, merged_df, condition):
        """
        Append the earliest LOCK/M9 release-date row to each product.

        Runs only when both ``'M9_Release_Date'`` and ``'Material_Status'``
        are present. For every product name:

        - If ``condition`` is True, the product's rows are first filtered to
          ``Material_Status == 'UNDEFINED'`` and ``Pre_M9 == 'No'``.
        - The earliest ``M9_Release_Date`` among rows with
          ``Material_Status == 'LOCK'`` and ``Quality_Milestone == 'M9'`` is
          found in ``merged_df`` and appended as a summary row (all columns
          blank except Product Name, Material_Status and M9_Release_Date).
        - If no such row exists, a placeholder row stating
          ``"No M9 release date found with Material Status=LOCK"`` is appended.

        Args:
            grouped_csv (pd.DataFrame): Grouped result from :meth:`group_data`.
            merged_df (pd.DataFrame): Full (ungrouped) query result used to
                look up LOCK/M9 rows.
            condition (bool): Whether to apply the UNDEFINED / Pre_M9='No'
                filter (see :meth:`update_pre_m9_condition`).

        Returns:
            pd.DataFrame: Product rows each followed by their LOCK/M9 summary
            row, or a copy of ``grouped_csv`` if the required columns are
            missing.
        """
        try:
            product_list = list(set(list(grouped_csv['Product Name'])))
            columns = list(grouped_csv.columns)
            result_df = pd.DataFrame(columns=columns)
            if "M9_Release_Date" in columns and "Material_Status" in columns:
                for prod_name in product_list:
                    prod_df =  grouped_csv[grouped_csv["Product Name"] == prod_name]
                    if condition:
                        prod_df = prod_df[
                            (prod_df["Material_Status"] == "UNDEFINED") & (prod_df["Pre_M9"] == "No")]
                    # prod_df = self.preserve_lock_premi9_row(prod_df)
                    # prod_df = self.handle_undefined_rows_with_lock(prod_df, org_columns)
                    prod_filtered = merged_df[
                        (merged_df["Product Name"] == prod_name) &
                        (merged_df["Material_Status"] == "LOCK") &
                        (merged_df["Quality_Milestone"] == "M9") &
                        (merged_df["M9_Release_Date"].notna())  # ✅ Exclude NaN/NaT dates
                        ]
                    if prod_filtered.shape[0] > 0:
                        min_date = prod_filtered["M9_Release_Date"].min()
                        df_filtered = prod_filtered[prod_filtered["M9_Release_Date"] == min_date]
                        lock_filtered = pd.DataFrame(columns=columns)
                        lock_filtered.loc[0, "Material Status"] = ""
                        lock_filtered.loc[0, "M9_Release_Date"] = "No M9 release date found with Material Status=LOCK"
                        lock_filtered = lock_filtered.fillna("")
                        if len(df_filtered) > 0:
                            df_filtered = df_filtered.head(1)
                            if df_filtered['Quality_Milestone'].iloc[0] != 'M9':
                                df_filtered.loc[:,'M9_Release_Date'] = "No M9 release date found with Material Status=LOCK"
                                df_filtered.loc[:,'Material_Status'] = ""


                            columns_to_keep = ["M9_Release_Date", "Material_Status", "Product Name"]
                            columns_to_empty = [col for col in df_filtered.columns if col not in columns_to_keep]
                            df_filtered[columns_to_empty] = ""
                            lock_filtered = df_filtered[columns]
                    else:
                        lock_filtered = pd.DataFrame(columns=columns)
                        lock_filtered.loc[0, "M9_Release_Date"] = "No M9 release date found with Material Status=LOCK"
                        lock_filtered.loc[0, "Product Name"] = prod_name
                        lock_filtered = lock_filtered.fillna("")

                    merged_row = pd.concat([prod_df, lock_filtered], ignore_index=True)

                    result_df = pd.concat([result_df, merged_row], ignore_index=True)
            else:
                result_df = copy.deepcopy(grouped_csv)
        except Exception:
            print(f'Error in group_data {traceback.print_exc()}')
        return result_df

    def sorting_data(self, grouped_csv, query_product_list, org_columns):
        """
        Order the output rows to follow the user's input order.

        For each identifier in ``query_product_list`` (matched against
        Product Name, SP Number or OPN):

        - If an "Invalid ..." row exists for it (in the 4th column), that row
          is used.
        - Otherwise all rows of the matching product are added, sorted by
          SP Number descending.

        Finally the columns are reordered to ``org_columns``, rows that are
        empty apart from Product Name are dropped, NaNs are replaced with
        ``""`` and duplicate rows are removed.

        Args:
            grouped_csv (pd.DataFrame): Fully processed result dataframe.
            query_product_list (list): Identifiers in the user's input order.
            org_columns (list): Column order from the user's input file.

        Returns:
            pd.DataFrame: The sorted, cleaned output dataframe (empty if no
            matches were found).
        """
        condition = 'default'
        try:
            result_df = pd.DataFrame()  # Empty dataframe to store results

            for identifier in query_product_list:
                # Check if identifier exists in any of the 3 columns
                invalid_result = grouped_csv.loc[
                    (
                            (grouped_csv['Product Name'] == identifier) |
                            (grouped_csv['SP Number'] == identifier) |
                            (grouped_csv['OPN'] == identifier)
                    )
                    &
                    (grouped_csv[list(grouped_csv.columns)[3]].astype(str).str.contains('Invalid', case=False, na=False))
                    ]
                if invalid_result.shape[0]>0:
                    result_df = pd.concat([result_df, invalid_result], ignore_index=True)
                else:
                    result = grouped_csv.loc[
                        (grouped_csv['Product Name'] == identifier) |
                        (grouped_csv['SP Number'] == identifier) |
                        (grouped_csv['OPN'] == identifier),
                        'Product Name'
                    ]

                    product_name = result.iloc[0] if not result.empty else None
                    if product_name != None:
                        filtered_df = grouped_csv[grouped_csv["Product Name"] == product_name]
                        filtered_df = filtered_df.sort_values(by='SP Number', ascending=False)

                        # Merge into result_df if match found
                        if not filtered_df.empty:
                            result_df = pd.concat([result_df, filtered_df], ignore_index=True)
                        else:
                            print(f"No match found for identifier: {identifier}")  # Log if not found

            if len(result_df) != 0:
                result_df = result_df.reindex(columns=org_columns)
                other_cols = result_df.columns.drop('Product Name')

                subset = result_df[other_cols].replace(r'^\s*$', pd.NA, regex=True)
                result_df = result_df[subset.notna().any(axis=1)]
                result_df = result_df.fillna("")

                result_df = result_df.drop_duplicates().reset_index(drop=True)
        except Exception:
            print(f'Error in sorting_data {traceback.print_exc()}')

        return result_df

    def update_pre_m9_condition(self, org_columns, input_dict):
        """
        Decide whether the UNDEFINED / Pre_M9 = 'No' filter should be applied.

        The filter is enabled only when the uploaded file's columns differ
        from the standard header (``config.mulprod_header_columns``) and the
        request's ``'pre_m9'`` option is ``'default'``.

        Args:
            org_columns (list): Columns from the uploaded file.
            input_dict (dict): Request payload; must contain ``'pre_m9'``.

        Returns:
            bool: True if the filter should be applied, otherwise False.
        """
        condition = False
        try:
            column_list = config.mulprod_header_columns
            status = sorted(column_list) == sorted(org_columns)
            if not status:
                if input_dict['pre_m9'] == 'default':#'Pre_M9' in org_columns and 
                    condition = True
        except Exception:
            print(f'Error in validate_standard_format')
        return condition

    def update_m9_date(self, actual_result_df, new_row, prod_name):
        """
        Append the earliest LOCK/M9 release-date row for a product.

        Looks up rows in ``actual_result_df`` for ``prod_name`` with
        ``Material_Status == 'LOCK'``, ``Quality_Milestone == 'M9'`` and a
        non-null ``M9_Release_Date``, takes the one with the earliest date and
        blanks every column except Product Name, M9_Release_Date and
        Material_Status. If none exists, a placeholder row stating
        ``"No M9 release date found with Material Status=LOCK"`` is used.

        Args:
            actual_result_df (pd.DataFrame): Full query result.
            new_row (pd.DataFrame): Row(s) to append the release-date row to.
            prod_name (str): Product name to look up.

        Returns:
            pd.DataFrame: ``new_row`` with the release-date row appended.
        """
        try:
            prod_filtered = actual_result_df[
                (actual_result_df['Product Name'] == prod_name) &
                (actual_result_df["Material_Status"] == "LOCK") &
                (actual_result_df["Quality_Milestone"] == "M9") &
                (actual_result_df["M9_Release_Date"].notna())  # ✅ Exclude NaN/NaT dates
                ]
            if prod_filtered.shape[0] > 0:
                min_date = prod_filtered["M9_Release_Date"].min()
                df_filtered = prod_filtered[prod_filtered["M9_Release_Date"] == min_date].head(1)
                cols_to_clear = [col for col in df_filtered.columns if col not in ['Product Name', 'M9_Release_Date','Material_Status']]
                df_filtered[cols_to_clear] = ""
            else:
                df_filtered = pd.DataFrame(columns=new_row.columns)
                df_filtered.loc[0, "M9_Release_Date"] = "No M9 release date found with Material Status=LOCK"
                df_filtered.loc[0, "Product Name"] = prod_name
                df_filtered = df_filtered.fillna("")

            new_row = pd.concat([new_row, df_filtered], ignore_index=True)

        except Exception:
            print(f'Error in update_m9_date {traceback.print_exc()} ')
        return new_row

    def update_invalid_identifier(self, actual_result_df, grouped_csv, org_columns, missed_product, act_sp_list, act_opn_list, act_sales_list, before_condition_list, after_condition_list):
        """
        Add rows for identifiers that are missing from the processed result.

        For each missed identifier, the input column it came from (SP Number,
        OPN or Product Name) decides the label:

        - If it was not found in the database at all, an
          ``"Invalid SP Number"`` / ``"Invalid OPN"`` /
          ``"Invalid Product Name"`` row is added (message in the 4th column).
        - If it exists in the database but was filtered out by the business
          rules, a row with ``"Product = <Quality_Milestone>"`` (or
          ``"Product = N/A"``) is added, followed by its LOCK/M9 release-date
          row via :meth:`update_m9_date`.

        Args:
            actual_result_df (pd.DataFrame): Full query result.
            grouped_csv (pd.DataFrame): Current output to append rows to.
            org_columns (list): Column order from the uploaded file.
            missed_product (list): Identifiers not present in the result.
            act_sp_list (list): SP Numbers from the input file (upper-cased).
            act_opn_list (list): OPNs from the input file (upper-cased).
            act_sales_list (list): Product Names from the input file
                (upper-cased).
            before_condition_list (list): Identifiers to check membership
                against for the "invalid" test.
            after_condition_list (list): Identifiers found in the database;
                if empty, every missed identifier is treated as invalid.

        Returns:
            pd.DataFrame: ``grouped_csv`` with the extra rows appended.
        """
        try:
            for product in missed_product:
                if product in act_sp_list:
                    if (product in before_condition_list and (product not in after_condition_list)) or len(after_condition_list) == 0 :
                        new_row = pd.DataFrame(columns=org_columns)
                        new_row.loc[0, ['SP Number', org_columns[3]]] = [product, "Invalid SP Number"]
                    else:
                        new_row = actual_result_df[
                            (
                                    (actual_result_df['Product Name'] == product) |
                                    (actual_result_df['OPN'] == product) |
                                    (actual_result_df['SP Number'] == product)
                            ) &
                            (actual_result_df['SP Number'].notna()) &
                            (actual_result_df['SP Number'] != '') &
                            (~actual_result_df['SP Number'].duplicated())
                            ]
                        sales_name = str(new_row['Product Name'].values[0])
                        new_row[org_columns[3]] = new_row['Quality_Milestone'].apply(
                            lambda v: f"Product = {v}" if pd.notna(v) and v != '' else 'Product = N/A'
                        )
                        keep_columns = ['Product Name','SP Number', 'OPN',org_columns[3]]
                        cols_to_clear = [col for col in new_row.columns if col not in keep_columns]
                        new_row[cols_to_clear] = ""
                        if sales_name != "" and 'M9_Release_Date' in new_row.columns:
                            new_row = self.update_m9_date(actual_result_df, new_row, sales_name)

                    grouped_csv = pd.concat([grouped_csv, new_row], ignore_index=True)

                elif product in act_opn_list:
                    if (product in before_condition_list and (product not in after_condition_list)) or len(after_condition_list) == 0 :
                        new_row = pd.DataFrame(columns=org_columns)
                        new_row.loc[0, ['OPN', org_columns[3]]] = [product, "Invalid OPN"]
                    else:
                        new_row = actual_result_df[
                            (
                                    (actual_result_df['Product Name'] == product) |
                                    (actual_result_df['OPN'] == product) |
                                    (actual_result_df['SP Number'] == product)
                            ) &
                            (actual_result_df['SP Number'].notna()) &
                            (actual_result_df['SP Number'] != '') &
                            (~actual_result_df['SP Number'].duplicated())
                            ]

                        sales_name = str(new_row['Product Name'].values[0])
                        new_row[org_columns[3]] = new_row['Quality_Milestone'].apply(
                            lambda v: f"Product = {v}" if pd.notna(v) and v != '' else 'Product = N/A'
                        )
                        keep_columns = ['Product Name','OPN', 'SP Number', org_columns[3]]
                        cols_to_clear = [col for col in new_row.columns if col not in keep_columns]
                        new_row[cols_to_clear] = ""
                        if sales_name != "" and 'M9_Release_Date' in new_row.columns:
                            new_row = self.update_m9_date(actual_result_df, new_row, sales_name)

                    grouped_csv = pd.concat([grouped_csv, new_row], ignore_index=True)

                elif product in act_sales_list:

                    if (product in before_condition_list and (product not in after_condition_list)) or (len(after_condition_list) == 0):
                        new_row = pd.DataFrame(columns=org_columns)
                        new_row.loc[0, ['Product Name', org_columns[3]]] = [product, "Invalid Product Name"]
                    else:
                        new_row = actual_result_df[
                            (
                                    (actual_result_df['Product Name'] == product) |
                                    (actual_result_df['OPN'] == product) |
                                    (actual_result_df['SP Number'] == product)
                            ) &
                            (actual_result_df['SP Number'].notna()) &
                            (actual_result_df['SP Number'] != '') &
                            (~actual_result_df['SP Number'].duplicated())
                            ]
                        sales_name = str(new_row['Product Name'].values[0])
                        new_row[org_columns[3]] = new_row['Quality_Milestone'].apply(
                            lambda v: f"Product = {v}" if pd.notna(v) and v != '' else 'Product = N/A'
                        )
                        keep_columns = ['Product Name', 'SP Number', 'OPN', org_columns[3]]
                        cols_to_clear = [col for col in new_row.columns if col not in keep_columns]
                        new_row[cols_to_clear] = ""
                        if sales_name != "" and 'M9_Release_Date' in new_row.columns:
                            new_row = self.update_m9_date(actual_result_df, new_row, sales_name)

                    grouped_csv = pd.concat([grouped_csv, new_row], ignore_index=True)

        except Exception:
            print(f'Error in update_invalid_identifier {traceback.print_exc()}')
        return grouped_csv

    def process_grouped_csv(self, grouped_csv):
        """
        Remove duplicate entries inside the location code/name columns.

        Column names are stripped first. Then, for every location column in
        ``column_pairs`` (codes and names) that exists, each cell is split on
        newlines and on ``'; '``; exact duplicate parts are dropped (first
        occurrence kept) and the parts are re-joined with newlines. Splitting
        on ``'; '`` rather than ``','`` keeps location names that contain
        commas intact. All other columns are left untouched.

        Args:
            grouped_csv (pd.DataFrame): Output dataframe to clean.

        Returns:
            pd.DataFrame: The cleaned dataframe (or the input unchanged on
            error or when no location columns are present).
        """
        try:
            #  Step 1: Strip spaces from column names
            grouped_csv.columns = grouped_csv.columns.str.strip()

            #  Step 2: Get both keys and values from column_pairs as a single list
            column_list = list(column_pairs.keys()) + list(column_pairs.values())

            #  Step 3: Remove duplicates from column_list (preserve order)
            column_list = list(dict.fromkeys(column_list))

            #  Step 4: Filter only existing columns
            valid_columns = [col for col in column_list if col in grouped_csv.columns]
            missing_columns = [col for col in column_list if col not in grouped_csv.columns]

            if missing_columns:
                print(f" Warning: Missing columns skipped: {missing_columns}")

            if not valid_columns:
                print(" No valid columns found to groupby!")
                return grouped_csv

            #  Step 5: Define inner dedup function
            def remove_duplicates_from_joined(values):
                """
                Deduplicate newline- and '; '-separated parts across a Series.

                Args:
                    values (pd.Series): Values to split and deduplicate.

                Returns:
                    str: Unique parts in first-seen order, joined with '\\n'.
                """
                all_parts = []
                seen = set()

                for val in values.dropna().astype(str):
                    #  Split by '\n' first, then by '; ' to handle joined values
                    lines = val.split('\n')
                    for line in lines:
                        #  Split by '; ' to separate multiple locations
                        parts = [p.strip() for p in line.split('; ')]
                        for part in parts:
                            if part and part not in seen:  # ✅ Check exact full location name
                                seen.add(part)
                                all_parts.append(part)

                return '\n'.join(all_parts)

            #  Step 6: Apply groupby only on valid columns
            for col in valid_columns:
                if col in grouped_csv.columns:
                    grouped_csv[col] = grouped_csv[col].apply(
                        lambda x: remove_duplicates_from_joined(pd.Series([x]))
                    )

            return grouped_csv

        except KeyError as e:
            print(f" KeyError: {e}")
            return grouped_csv
        except Exception as e:
            print(f" Unexpected Error: {e}")
            return grouped_csv

    def process_multiple_product(self, input_dict):
        """
        Run the full multiple-product lookup and build the Excel output.

        Steps:

        1. Read the uploaded file and work out the requested columns (plus
           the mandatory columns needed for processing).
        2. Take the first non-empty identifier per row (Product Name,
           SP Number or OPN) and query the database with exact matching.
        3. Rename columns to display names, group location data
           (:meth:`group_data`), add LOCK/M9 release-date rows
           (:meth:`update_lock_status`) and add rows for missing or invalid
           identifiers (:meth:`update_invalid_identifier`).
        4. Post-process: deduplicate location values, relabel
           ``"Product = M9"`` as ``"Product = Obsoleted"``, collapse UNDEFINED
           rows under LOCK (:meth:`handle_undefined_rows_with_lock`) and sort
           to the input order (:meth:`sorting_data`).
        5. Export the result to Excel.

        Args:
            input_dict (dict): Request payload with at least ``'filepath'``,
                ``'message_id'`` and ``'pre_m9'``.

        Returns:
            dict: ``input_dict`` with ``'final_answer'`` set to the Excel file
            bytes, or ``None`` if nothing was produced.
        """
        input_dict['final_answer'] = None
        mandatory_columns = ["M9_Release_Date", "Material_Status", "Quality_Milestone", 'Wafer_Fab_Loc', 'Wafer_Fab_Loc_Name', 'Wafer_Test_Loc', 'Wafer_Test_Loc_Name', 'Assembly_Loc', 'Assembly_Loc_Name', 'Final_Test_Loc', 'Final_Test_Loc_Name', 'Pre_M9']
        actual_result_df = pd.DataFrame()
        try:
            dataframe, org_columns = self.read_input_file(input_dict)
            condition = self.update_pre_m9_condition(org_columns, input_dict)
            columns = org_columns + [item for item in mandatory_columns if item not in org_columns]
            filtered_dict = {k: v for k, v in column_mapping.items() if v in columns}
            df = copy.deepcopy(dataframe[['Product Name', 'SP Number', 'OPN']])
            act_sp_list = [item.strip().upper() for item in set(df['SP Number']) if isinstance(item, str)]
            act_opn_list = [item.strip().upper() for item in set(df['OPN']) if isinstance(item, str)]
            act_sales_list = [item.strip().upper() for item in set(df['Product Name']) if isinstance(item, str)]
            query_product_list = df.apply(lambda row: row.dropna().iloc[0] if row.dropna().size else None, axis=1).tolist()
            query_product_list = [str(item).upper() if item is not None else None for item in query_product_list]
            if len(query_product_list) > 0:
                query = self.generate_sql_query(query_product_list, "exact")
                response = sql_service.select_product_db(query)
                if len(response) > 0:
                    merged_df = copy.deepcopy(response)
                    output_folder = os.path.join("process_id", input_dict['message_id'], "final_response")
                    os.makedirs(output_folder, exist_ok=True)
                    filtered_columns = merged_df[list(filtered_dict.keys())]
                    merged_df = filtered_columns.rename(columns=column_mapping, errors='ignore')
                    merged_df = merged_df.drop_duplicates()
                    merged_df = merged_df.reset_index(drop=True)
                    before_condition_list = merged_df[['SP Number', 'OPN', 'Product Name']].values.flatten().tolist()
                    before_condition_list = [item for item in before_condition_list if pd.notna(item)]
                    actual_result_df = copy.deepcopy(merged_df)
                    # if condition :
                    #     merged_df = merged_df[(merged_df["Material_Status"] == "UNDEFINED") & (merged_df["Pre_M9"] == "No")]
                    sp_list = [item.strip() for item in set(merged_df['SP Number']) if item is not None]
                    opn_list = [item.strip() for item in set(merged_df['OPN']) if item is not None]
                    sales_list = [item.strip() for item in set(merged_df['Product Name']) if item is not None]
                    after_condition_list = [*sp_list, *opn_list, *sales_list]
                    query_product_list = [item.strip() for item in query_product_list]
                    after_condition_list = [item.strip() for item in after_condition_list]
                    missed_product = list(
                        set([p.upper() for p in query_product_list]) -
                        set([p.upper() for p in after_condition_list])
                    )
                    # missed_product = list(set(query_product_list) - set(after_condition_list))
                    if merged_df.shape[0] > 0:
                        grouped_csv = self.group_data(merged_df)
                        grouped_csv = self.update_lock_status(grouped_csv, merged_df, condition)
                        grouped_csv = grouped_csv.reindex(columns=org_columns)
                        if len(missed_product) > 0:
                            grouped_csv = self.update_invalid_identifier(actual_result_df, grouped_csv, org_columns, missed_product, act_sp_list, act_opn_list, act_sales_list, query_product_list, before_condition_list)

                    else:
                        grouped_csv = self.update_invalid_identifier(actual_result_df, pd.DataFrame(), org_columns, missed_product,
                                                                     act_sp_list, act_opn_list, act_sales_list, query_product_list, before_condition_list)
                else:
                    grouped_csv = self.update_invalid_identifier(actual_result_df, pd.DataFrame(), org_columns,
                                                                 query_product_list,
                                                                 act_sp_list, act_opn_list, act_sales_list,
                                                                 [], [])
            else:
                grouped_csv = self.update_invalid_identifier(actual_result_df, dataframe, org_columns, query_product_list,
                                                             act_sp_list, act_opn_list, act_sales_list, [], [])


            # Applying Post processing
            # Remove duplicate
            grouped_csv = self.process_grouped_csv(grouped_csv)
            # Update Obsolete value
            if len(grouped_csv.columns) > 3:
                grouped_csv[grouped_csv.columns[3]] = grouped_csv[grouped_csv.columns[3]].replace('Product = M9',
                                                                                              'Product = Obsoleted')
            grouped_csv = grouped_csv.drop_duplicates().fillna('N/A')
            grouped_csv = self.handle_undefined_rows_with_lock(grouped_csv, org_columns)
            sorted_df = self.sorting_data(grouped_csv, query_product_list, org_columns)

            if len(sorted_df) != 0:
                excel_bytes = export.generate_final_excel(sorted_df)
                # with open(target_filename, "wb") as f:
                #     f.write(excel_bytes)
                input_dict['final_answer'] = excel_bytes

        except Exception:
            print(f'Error In process_multiple_product {traceback.print_exc()}')

        return input_dict

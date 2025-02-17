import polars as pl


class Difftool:

    @staticmethod
    def diff_update(
        update: pl.DataFrame,
        destination: pl.DataFrame,
        column_names_props: list[str],
        key_columns: list[str],
        ignore_dates=False,
    ) -> tuple[list[pl.DataFrame], set[str]]:
        """
        This function compares two dataframes and returns a list of dataframes with the differences and a set with the keys that have differences across the whole dataframe.
        It is not very efficient, but very helpful when doing data validation on a sync that is going to affect an already existing dataset.

        Args:
            update (pl.DataFrame): The dataframe returned by the beetl comparer containing the items going to be updated.
            destination (pl.DataFrame): The destination dataframe after being transformed using the source transformers.
            column_names_props (list[str]): The list of column names that are going to be compared.
            key_columns (list[str]): The list of columns that are going to be used as keys.
            ignore_dates (bool, optional): If true, it will ignore columns that have "date" in their name. Defaults to False.

        Returns:
            tuple[list[pl.DataFrame], set[str]]: A tuple containing a list of dataframes with the differences and a set with the keys that have differences across the whole dataframe
        """
        column_names = [*column_names_props]
        if ignore_dates:
            column_names = [name for name in column_names if "date" not in name.lower()]

        calcultated_dataframe_diffs = []
        keys_that_differ_across_whole_dataset = set()

        for row_index in range(update.height):
            current_row_keys_dataframe = update.select(key_columns)[row_index]
            current_update_dataframe_row = update.join(
                current_row_keys_dataframe, on=key_columns
            ).select(column_names)
            current_destination_dataframe_row = destination.join(
                current_row_keys_dataframe, on=key_columns
            ).select(column_names)

            multiple_rows_found_using_keys = (
                current_update_dataframe_row.height > 1
                or current_destination_dataframe_row.height > 1
            )
            if multiple_rows_found_using_keys:
                raise Exception(
                    f"Multiple rows were found when using the keys {key_columns} for row index {row_index} in the update dataset. This is not allowed since it will cause beetl to miss or overwrite data that should't be overwritten."
                )

            current_row_comparison_dataframe = (
                current_update_dataframe_row == current_destination_dataframe_row
            )
            current_row_comparison_dict = current_row_comparison_dataframe.to_dict()
            keys_that_differ_in_current_row = []
            for key, value in current_row_comparison_dict.items():
                if not value[0]:
                    keys_that_differ_in_current_row.append(key)

            key_values_for_current_row = list(
                map(str, list(current_row_keys_dataframe.to_dicts()[0].values()))
            )

            current_diff_dataframe = pl.DataFrame().with_columns(
                pl.Series("dataset", ["update", "destination"]),
                pl.Series(
                    "keys", [key_values_for_current_row, key_values_for_current_row]
                ),
            )
            current_row_has_diff = False
            for key in keys_that_differ_in_current_row:
                value1 = current_update_dataframe_row[key][0]
                value2 = current_destination_dataframe_row[key][0]
                if value1 is None and value2 is None:
                    continue
                if value1 == value2:
                    continue
                series = pl.Series(key, [value1, value2])
                current_diff_dataframe = current_diff_dataframe.with_columns(series)
                current_row_has_diff = True

            if not current_row_has_diff:
                continue

            calcultated_dataframe_diffs.append(current_diff_dataframe)
            keys_that_differ_across_whole_dataset.update(
                keys_that_differ_in_current_row
            )

        return (calcultated_dataframe_diffs, keys_that_differ_across_whole_dataset)

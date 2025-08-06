import os

import pyarrow.parquet as pq


def parquet_merger(input_paths: list, output_path: str) -> None:
    """
    Concatenates multiple Parquet files into a single file,
    processing row-groups sequentially.

    Args:
        input_paths (list): List of input file paths.
        output_path  (str): Output file path.
    """

    if not input_paths:
        raise ValueError('No input files provided.')
    elif not output_path:
        raise ValueError('No output path provided.')

    # Open the first file to get the Arrow schema
    first_file = pq.ParquetFile(input_paths[0])
    schema = first_file.schema.to_arrow_schema()

    # Initialize the Parquet writer with Arrow schema
    writer = pq.ParquetWriter(output_path, schema)

    try:
        for path in input_paths:
            # Convert to Arrow schema
            file = pq.ParquetFile(path)
            current_schema = file.schema.to_arrow_schema()

            # Verify schema compatibility
            if not current_schema.equals(schema):
                raise ValueError(
                    f'Schema of {os.path.basename(path)} does not match the '
                    f'first file.')

            # Process each row-group individually
            for rg_index in range(file.num_row_groups):
                table = file.read_row_group(rg_index)
                writer.write_table(table)

    finally:
        writer.close()

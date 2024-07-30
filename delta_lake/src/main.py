import sys
import os
import argparse
from delta_lake.utils.fake_data import generate_fake_supply_chain_data,dataframe_to_parquet_buffer,manipulate_data,generate_fake_mul_cols_supply_chain_data
from delta_lake.utils.read_data import dataframe_to_file,read_file,read_yaml
import yaml
if __name__ == "__main__":
    # Example usage:
    file_path = 'config/delta.yaml'  # Replace with your YAML file path
    yaml_data = read_yaml(file_path)
    parser = argparse.ArgumentParser(description='Process some integers.')
    parser.add_argument('--num_rows', type=int, default=100, help='Number of rows for fake supply chain data')
    parser.add_argument('--num_cols', type=int, default=8, help='Number of cols for fake supply chain data')
    parser.add_argument('--output_format', type=str, default='parquet', choices=['parquet', 'csv.gz', 'html', 'xlsx'], help='Output format')
    parser.add_argument('--base_dir', type=str, default=yaml_data['delta_data']['base_path'], help='Base directory')
    parser.add_argument('--fake_mode', type=str, default="multi_rows", choices=['multi_rows', 'multi_cols'],help='fake mode')

    args = parser.parse_args()
    # if args.fake_mode == 'multi_rows':
    #     # Generate fake supply chain data
    #     df = generate_fake_supply_chain_data(args.num_rows)
    # elif args.fake_mode == 'multi_cols':
    #     df = generate_fake_mul_cols_supply_chain_data(args.num_rows)
    # # Convert DataFrame to Parquet format and write to buffer
    # data_buffer = dataframe_to_parquet_buffer(df)
    # # Convert buffer to specified output format and save with current date in directory structure
    # file_path = dataframe_to_file(data_buffer, output_format=args.output_format, base_dir=args.base_dir)
    # df = read_file(file_path)
    # print(df.head())
    # print(df.info())
    

    # update the file data
    source_path = "data/scmp-raw-layer/cpo/cpis/ba_dmnd_data/year=2024/month=7/day=29/ba_dmnd_data_20240729_mulcols.parquet"
    source_df = read_file(source_path)
    source_df_unique = source_df.drop_duplicates(subset=["order_id"])
    print(len(source_df_unique))
    print(len(source_df))
    print(source_df.head(15))
    print(source_df.info())
    updated_df = manipulate_data(source_df,args.fake_mode, "mixed")
    print(len(updated_df))
    data_buffer = dataframe_to_parquet_buffer(updated_df)
    file_path = dataframe_to_file(data_buffer, args.fake_mode,args.output_format, base_dir=args.base_dir)
    print(updated_df.head(15))
    print(updated_df.info())



# python3 src/main.py --num_rows 10 --output_format parquet

# 26 original
# 27 update  ORD00950   
# 25 upsert ORD0008 ORD0011
# 26 delete ORD00312 
# 27 mixed 10,000,999


# 26 original
# 27 update  ORD0003   
# 25 upsert ORD0008 upsert ORD0011
# 26 delete ORD0001
# 27 mixed 10 delete ORD0005 update ORD0003 upsert ORD0012
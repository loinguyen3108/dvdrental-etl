import re
from argparse import ArgumentParser
from datetime import datetime

from jobs.transform.transform import TransformETL

arg_parser = ArgumentParser(description='Data ETL')
arg_parser.add_argument(
    '--exec-date', dest='execution_date',
    type=str, help='Date Format: YYYY-MM-DD.')
arg_parser.add_argument(
    '--p-keys', dest='p_keys',
    type=str, help='Column name to partition. Format: arg1,arg2,...')
arg_parser.add_argument(
    '--table-name', dest='table_name',
    type=str, help='Table need etl.')
args = arg_parser.parse_args()
exec_date = datetime.fromisoformat(args.execution_date)
p_keys = re.split(r',\s*', args.p_keys.strip())
table_name = args.table_name

etl = TransformETL(exec_date=exec_date)
etl.run(tb_name=table_name, partition_key=p_keys)

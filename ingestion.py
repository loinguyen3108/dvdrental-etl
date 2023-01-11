from argparse import ArgumentParser
from datetime import datetime

from jobs.ingestion.ingestion import Ingestion


arg_parser = ArgumentParser(description='Data Ingestion')
arg_parser.add_argument(
    '--exec-date', dest='execution_date',
    type=str, help='Date Format: YYYY-MM-DD.')
arg_parser.add_argument(
    '--loading-type', dest='loading_type',
    type=str, help='Choice: [by_date, by_id].')
arg_parser.add_argument(
    '--p-key', dest='p_key',
    type=str, help='Column name filter.')
arg_parser.add_argument(
    '--table-name', dest='table_name',
    type=str, help='Table need ingest.')
args = arg_parser.parse_args()
exec_date = datetime.fromisoformat(args.execution_date)
loading_type = args.loading_type
p_key = args.p_key
table_name = args.table_name

ingestion = Ingestion()
ingestion.run(table_name=table_name, exec_date=exec_date,
              loading_type=loading_type, p_key=p_key)

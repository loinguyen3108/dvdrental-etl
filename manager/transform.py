from argparse import ArgumentParser
from datetime import datetime

from etl.jobs.transform.transform import TransformETL


arg_parser = ArgumentParser(description='Data ETL')
arg_parser.add_argument(
    '--exec-date', dest='execution_date',
    type=str, help='Date Format: YYYY-MM-DD.')
args = arg_parser.parse_args()
exec_date = datetime.fromisoformat(args.execution_date)

etl = TransformETL(exec_date=exec_date)
etl.run()

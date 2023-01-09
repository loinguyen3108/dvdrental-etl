from jobs.ingestion.ingestion import Ingestion


ingestion = Ingestion()
ingestion.run(table_name='film')

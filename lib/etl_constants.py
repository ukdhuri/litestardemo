
TABLE_BCP_EXTRACTOR='table_bcp_extractor'
MANDATORY_TABLE_BCP_EXTRACTOR_PROPERTIES = ['table','file']
MANDATORY_SQL_BCP_EXTRACTOR_PROPERTIES = ['sqlfile','file']
MANDATORY_TABLE_BCP_LOADER_PROPERTIES = ['table','load_type','file']
MANDATORY_TABLE_PANDAS_LOADER_PROPERTIES = ['table','load_type','file']
MANDATORT_FILE_PROPERTIES = ['file_path','file_prefix']
HEADERS_IN_ORDER='headers_in_order'
TRAILERS_IN_ORDER='trailers_in_order'
SQL_BCP_EXTRACTOR='sql_bcp_extractor'
known_components = ['file','table','sqlfile']
plular_components_mapping = {'file':'files','table':'tables'}
SQL_PANDA_EXTRACTOR='sql_panda_extractor'
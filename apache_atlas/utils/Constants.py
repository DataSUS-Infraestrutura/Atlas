from enum import Enum

class TypeNames(Enum):
    DATA_REPOSITORY = "dt_data_repository"
    DATABASE = "dt_database"
    TABLE_COLUMN = "dt_table_column"
    TABLE = "dt_table"
    TABLE_FILE = "dt_table_file"
    # Mudar depois
    ANUAL_TABLE = "dt_annual_table"
    MONTLY_TABLE = "dt_monthly_table"
    PROCESS = "Process"
    TIMELINE = "dt_timeline"
    PROCESS_CHANGE_COLUMN = "dt_column_change_process"

    def __str__(self):
        return str(self.value)    

class EndRelations:
    END_TABLE_TO_COLUMN = ('columns_table', 'belongs_to_table')
    END_LINEAGE_TO_COLUMN = ('columns_anual_table', 'belongs_to_table_anual')
    END_TABLE_TO_REFERENCE = ('references', 'is_reference_table')
    END_TABLE_TO_FILE = ('files', 'is_file_table')
    END_TABLE_TO_DOCUMENTATION_FILE = ('documentation_files', 'is_files_documentation_tables')
    END_DATABASE_TO_TABLE = ('tables', 'belongs_database')
    END_DATABASE_TO_SOURCE = ('sources', 'is_database_source')
    END_TABLE_FILE_COLUMN = ('columns_file_table', 'is_column_table_file')
    END_TIMELINE_TO_TABLE = ('table_interval', 'belongs_timeline')
    END_REPOSITORY_DATA = ('database_repository', 'belongs_data_repository')

    def __str__(self):
        return str(self.value)
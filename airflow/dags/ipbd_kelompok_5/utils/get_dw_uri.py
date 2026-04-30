import os


def get_dw_uri():
    DW_HOST = "dw"
    DW_PORT = "5432"
    DW_DB = os.getenv("DW_DB", "data_warehouse")
    DW_USER = os.getenv("DW_USER", "postgres")
    DW_PASSWORD = os.getenv("DW_PASSWORD", "postgres")

    return f"postgresql://{DW_USER}:{DW_PASSWORD}@{DW_HOST}:{DW_PORT}/{DW_DB}"

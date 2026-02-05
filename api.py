from automation.helpers import connect_to_sql, get_sql_con
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import uvicorn
import time
from sql_queries import get_sql_agg_most
from automation.codeeghtesadi import code_eghtesadi as codeeghtesadi
import asyncio
from concurrent.futures import ProcessPoolExecutor
from functools import partial
from automation.helpers import cleanup
from automation.constants import get_sql_con

app = FastAPI()
origins = ["*"]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


def get_most():
    sql_query = get_sql_agg_most()
    df = connect_to_sql(sql_query=sql_query, sql_con=get_sql_con(
        database='testdbV2'), read_from_sql=True, return_df=True, return_df_as='json')

    return df


@app.get("/arzeshafzoode")
async def get_sabtenam_arzesh_Afzoode():
    try:
        with ProcessPoolExecutor() as executor:
            partial_task = partial(codeeghtesadi, get_sabtenamCodeEghtesadiData=True,
                                   saving_dir='arzeshafzoodeh',
                                   down_url="https://reports.tax.gov.ir/Reports/RegistrationReports/AllTaxpayerDataWithVatInfo")
            # Use ProcessPoolExecutor to run the CPU-bound task in a separate process
            result = await asyncio.get_event_loop().run_in_executor(executor, partial_task)

        return "done"
    except Exception as e:
        print(e)
        return "Error"


@app.get("/sabtenamcodeeghtesadi")
async def get_sabtenam_code_eghtesadi():
    try:
        with ProcessPoolExecutor() as executor:
            partial_task = partial(codeeghtesadi, get_sabtenamCodeEghtesadiData=True,
                                   saving_dir='codeeghtesadi',
                                   down_url="https://reports.tax.gov.ir/Reports/RegistrationReports/FullTaxpayerInformationInExcel")
            # Use ProcessPoolExecutor to run the CPU-bound task in a separate process
            result = await asyncio.get_event_loop().run_in_executor(executor, partial_task)

        return "done"
    except Exception as e:
        print(e)
        return "Error"


@app.get("/cancel_task/{task_name}")
async def cancel_task(task_name):
    # Implement logic to cancel the task, e.g., set a flag
    # This will be specific to your application's design
    # In this example, we're using an asyncio.Event to signal cancellation
    df = connect_to_sql(sql_query=f"select * from tblAliveProcesses where name='{task_name}'",
                        read_from_sql=True, return_df=True, sql_con=get_sql_con(database='test'))
    pid = int(df.tail(10)['pid'])
    cleanup(pid)
    return {"message": "Task cancellation requested"}


if __name__ == "__main__":
    uvicorn.run(app, host='0.0.0.0', port=8000)

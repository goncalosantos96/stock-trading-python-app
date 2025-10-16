import schedule
from datetime import datetime
from script import extract_data_from_api_job
import time


def basic_job():
    print(f"Job run at: {datetime.now()}")

# Schedule to run basic_job function every 5 minutes
schedule.every(5).minutes.do(basic_job)


# Schedule to run extract_data_from_api_job every 5 minutes
schedule.every(5).minutes.do(extract_data_from_api_job)


while True:

    # Run jobs scheduled that are on time
    schedule.run_pending()

    # Wait 1 second until check 
    time.sleep(1)

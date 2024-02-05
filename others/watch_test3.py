import asyncio

lock = asyncio.Lock()

async def process_data(data):
    # Acquire the lock to ensure task safety
    async with lock:
        # Perform some processing on the data
        print(f"Processed datab: {data}")
        processed_data = data * 2

        # Sleep for 5 seconds
        await asyncio.sleep(1)
        # Print the processed data
        print(f"Processed dataf: {processed_data}")

async def main():
    # Create a list to store the tasks
    tasks = []

    # Define the number of times to call the function
    num_calls = 5

    # Create and start the tasks
    for _ in range(num_calls):
        task = asyncio.create_task(process_data("example data"))
        tasks.append(task)

    # Wait for all tasks to finish
    await asyncio.gather(*tasks)

async def main2():
    # Create a list to store the tasks
    tasks = []

    # Define the number of times to call the function
    num_calls = 5

    # Create and start the tasks immediately
    for _ in range(num_calls):
        task = asyncio.create_task(process_data("example data"))
        tasks.append(task)

    # Wait for all tasks to finish
    await asyncio.sleep(15)  # Allow other tasks to run

    # # Start the tasks
    # for task in tasks:
    #     await task

if __name__ == "__main__":
    asyncio.run(main2())
   



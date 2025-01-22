# Usage: locust -f locustfile.py --headless --users 10 --spawn-rate 1 -H http://localhost:8000
from locust import HttpUser, task, between


class QuickstartUser(HttpUser):
    wait_time = between(1, 1)

    # @task(10)
    # def home(self):
    #     self.client.get("/", name="/home")

    # @task(5)
    # def io_task(self):
    #     self.client.get("/io_task", name="/io_task")

    # @task(5)
    # def cpu_task(self):
    #     self.client.get("/cpu_task", name="/cpu_task")

    # @task(3)
    # def random_sleep(self):
    #     self.client.get("/random_sleep", name="/random_sleep")    

    # @task(10)
    # def random_status(self):
    #     self.client.get("/random_status", name="/random_status")

    # @task(3)
    def clickhouse_sync(self):
        self.client.get("/clickhouse_sync", name="/clickhouse_sync")

    # @task(3)
    def clickhouse_async(self):
        self.client.get("/clickhouse_async", name="/clickhouse_async")

    @task()
    def sleep_sync(self):
        self.client.get("/sleep_sync", name="/sleep_sync") 
    
    # @task()
    def sleep_async(self):
        self.client.get("/sleep_async", name="/sleep_async")

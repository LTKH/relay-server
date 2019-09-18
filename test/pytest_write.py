import requests
import random
from threading import Thread

def test_write():

    def new_request(result, i):
        for x in range(100000):
            v = random.uniform(20, 100)
            h = random.randint(10, 40)
            u = 'http://localhost:6086/write?db=test'
            d = 'cpu_load_short,host=server'+str(h)+',region=us-west value='+str(v)
            r = requests.post(u, data=d)
            print(u+' '+d+' ('+str(i)+'-'+str(x)+')')
            if r.status_code != 204:
                result[i] = False
                return False

        result[i] = True
        return True
        


    print("\n")

    threads = [None] * 1000
    results = [None] * 1000

    for i in range(len(threads)):
        threads[i] = Thread(target=new_request, args=(results, i))
        threads[i].start()

    for i in range(len(threads)):
        threads[i].join()

    for i in range(len(results)):
        if results[i] == False:
            assert False

    assert True

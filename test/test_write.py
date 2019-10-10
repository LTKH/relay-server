import requests
import random
from threading import Thread

def test_write():

    def new_request(result, i):
        for n in range(1000):
            u = 'http://localhost:6086/write?db=test'
            d = []

            for x in range(200):
                v = random.uniform(20, 100)
                h = random.randint(10, 40)
                h2 = random.randint(10, 100)
                r = random.randint(10, 100)
                d.append('cpu_load_short,host=server'+str(h)+',host2=server'+str(h2)+',host3=server'+str(h2)+',region='+str(r)+' value='+str(v))

            try:
                r = requests.post(u, data="\n".join(d))
            except:
                result[i] = False
                return False
            
            if r.status_code != 204:
                result[i] = False
                return False

            print(u+' - '+str(r.status_code))

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

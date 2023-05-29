from processor import WarcProcessor
import multiprocessing as mp
import time
import distcompute_client as dc
import os
import sys



def worker(n_proc):
    client = dc.init(url="http://213.173.104.204/", stage="a")
    processor = WarcProcessor()
    while True:

        if not client.is_alive():
            client = dc.init(url="http://213.173.104.204/", stage="a")
            continue

        if client.job_count() == 0:
            time.sleep(30)
            break
    
        try:
            client.new_job()
            jobname =  "https://data.commoncrawl.org/" + str(client.job)      # "s3://commoncrawl/"+ str (client.job)
            jobname = "s3://commoncrawl/"+ str(client.job)
        except:
            client.bye()
            break
        if str(client.job)[:5]!="crawl":
            continue
        

        print('job name: ', jobname) # str
        print('./BLID_Phase1_'+jobname.split("/")[-1].split(".")[-3]+'.parquet')
        print(client.job_id)
        
        s = time.time()
        processor.process_warc(jobname.split()[0], output_path='s3://s-laion/bild-test2')
        e = time.time()
        print(f'processed in: {e-s}')
        output = {"file": './BLID_Phase1_'+jobname.split("/")[-1].split(".")[-3]+'.parquet'}

        client.complete_job(output)
        # break


if __name__ == '__main__':
    worker(1)

 

import streaming_httplib2.dcache as cache
import sys
import time

if __name__ == "__main__":
    # Do not start too fast, as the master will take some time to create/populate the cache 
    filename = sys.argv[1]
    sleep0 = float(sys.argv[2])
    timeout = float(sys.argv[3])
    sleep1 = float(sys.argv[4])
    sleep2 = float(sys.argv[6]) if len(sys.argv) > 6 else 0.0
    finalSet = sys.argv[5] if len(sys.argv) > 5 else None
    startTime = time.time()
    # First sleep
    time.sleep(sleep0)

    # Then try to get the content of cache
    c = cache.DistributedFileCache("tmp/cache", create = True)
    initialTime = time.time()
#    print "GETTING", int(time.time() - startTime)
    v = c.get(filename, timeout = timeout)   
#    print "GETTING DONE", int(time.time() - startTime) 

    waited = time.time() - initialTime
    if v != None:
        v = v.read()
    print int(waited), v

    # Wait before dying
    time.sleep(sleep1)

    # If requested, set the content
    if finalSet != None:
        c.set(filename, finalSet, "")

    time.sleep(sleep2)


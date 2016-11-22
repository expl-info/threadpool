#! /usr/bin/env python
#! /usr/bin/env python3
#
# threadpooltests.py

# license--start
#
# Copyright 2012 John Marshall
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# license--end

from threadpool import ThreadPool

import random
import subprocess
import time

def random_generation(nworkers, count):
    """Generate random numbers. Reap results of known count.
    """
    tp = ThreadPool(nworkers)
    for i in range(count):
        tp.add(i, random.random)
    results = [tp.reap() for i in range(count)]
    print("\n".join(map(str, results)))

def random_generation_with_drain(nworkers, count):
    """Generate random numbers. Drain waiting requests (nworkers
    must be small enough so that not all tasks are completed by the
    time drain is called). Reap results of unknown count.
    """
    tp = ThreadPool(nworkers)
    for i in range(count):
        tp.add(i, random.random)
    tp.drain()
    results = []
    while not tp.is_empty():
        res = tp.reap()
        results.append(res)
    print("\n".join(map(str, results)))

def callexec(args, nworkers, count):
    """Call executable.
    """
    def run(*args):
        p = subprocess.Popen(args, stdout=subprocess.PIPE, close_fds=True)
        out, err = p.communicate()
        return out

    tp = ThreadPool(nworkers)
    for i in range(count):
        tp.add(i, run, args=args[:])
    outs = [tp.reap()[1].strip() for i in range(count)]
    print("\n".join(map(str, outs)))

if __name__ == "__main__":
    print("threadpooltests\n")

    if 1:
        print("\nrandom_generation:")
        t0 = time.time()
        random_generation(5, 500)
        print("elapsed (%s)" % str(time.time()-t0))

    if 1:
        print("\nrandom_generation_with_drain:")
        t0 = time.time()
        random_generation_with_drain(5, 500)
        print("elapsed (%s)" % str(time.time()-t0))

    if 1:
        print("""\ncallexec(["/bin/sleep", "1"], 5, 100)""")
        t0 = time.time()
        callexec(["/bin/sleep", "1"], 5, 100)
        print("elapsed (%s)" % str(time.time()-t0))

    if 1:
        print("""\ncallexec(["/bin/hostname"], 2, 100)""")
        t0 = time.time()
        callexec(["/bin/hostname"], 2, 100)
        print("elapsed (%s)" % str(time.time()-t0))

    if 1:
        print("""\ncallexec(["/bin/hostname"], 5, 100)""")
        t0 = time.time()
        callexec(["/bin/hostname"], 5, 100)
        print("elapsed (%s)" % str(time.time()-t0))

    if 1:
        print("""\ncallexec(["/bin/hostname"], 10, 100)""")
        t0 = time.time()
        callexec(["/bin/hostname"], 10, 100)
        print("elapsed (%s)" % str(time.time()-t0))

    if 1:
        print("""\ncallexec(["/bin/hostname"], 20, 100)""")
        t0 = time.time()
        callexec(["/bin/hostname"], 20, 100)
        print("elapsed (%s)" % str(time.time()-t0))

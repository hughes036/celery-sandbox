import time
from celigo_pipeline_poc.celery_consumer import add, ioBoundTask, cpuBoundTask
from functools import reduce


NUM_JOBS_DEFAULT = 50


def testAdd(x, y):
    responses = []
    for i in range(NUM_JOBS_DEFAULT):
        responses.append(add.delay(x, y))

    results = map(lambda r: r.get(), responses)
    sum = reduce(lambda x, y: x + y, results)
    print(sum)


def testIoBoundTask(num_jobs=NUM_JOBS_DEFAULT):
    start = time.time()
    responses = []
    for _ in range(NUM_JOBS_DEFAULT):
        responses.append(ioBoundTask.delay())

    results = map(lambda r: r.get(), responses)
    reduced_result = reduce(_reduce_result, list(results), {})
    # print(json.dumps(reduced_result))
    print(f"{num_jobs} Jobs | Time taken: {time.time() - start}")


def testCpuBoundTask(num_jobs=NUM_JOBS_DEFAULT):
    start = time.time()
    compute_results = []
    for _ in range(NUM_JOBS_DEFAULT):
        compute_results.append(cpuBoundTask.delay(5))

    results = list(map(lambda r: r.get(), compute_results))
    reduced_result = reduce(_reduce_result, list(results), {})
    # print(json.dumps(reduced_result))
    print(f"{num_jobs} Jobs | Time taken: {time.time() - start}")


def _reduce_result(acc, code):
    if code in acc:
        acc[code] += 1
    else:
        acc[code] = 1
    return acc


def main():
    # testAdd(2, 3)
    print("________Testing IO bound task________")
    # test of n = 1 to negate any caching effects
    testIoBoundTask(1)
    for num_jobs in [10, 50, 100, 200]:
        testIoBoundTask(num_jobs)
    print("________Testing CPU bound task________")
    for num_jobs in [10, 50, 100, 200]:
        testCpuBoundTask(num_jobs)


if __name__ == "__main__":
    main()

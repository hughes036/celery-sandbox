from tasks import add
from functools import reduce

responses = []
for i in range(10):
    responses.append(add.delay(2, 2))

results = map(lambda r: r.get(), responses)
sum = reduce(lambda x, y: x + y, results)
print(sum)
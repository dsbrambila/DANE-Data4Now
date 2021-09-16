import numpy as np
rdd = sc.parallelize(np.arange(0,100))
rdd_filter = rdd.filter(lambda x: x%2 == 0)
rdd_map = rdd_filter.map(lambda x: x*x )
rdd_map.collect()
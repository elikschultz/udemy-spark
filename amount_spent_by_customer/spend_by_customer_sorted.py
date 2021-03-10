from pyspark import SparkConf, SparkContext

# Spark setup
conf = SparkConf().setMaster('local').setAppName('total_spent_by_customer')
sc = SparkContext(conf = conf)

# Define function to use as map for inputs
def parse_line(line):
    split_line = line.split(',')
    customer_id = split_line[0]
    amount_spent = split_line[2]
    return (int(customer_id), float(amount_spent))
    
# Perform computations
lines = sc.textFile('customer-orders.csv')
parsed_lines = lines.map(parse_line)
spend_by_customer = parsed_lines.reduceByKey(lambda x, y: x + y)
spend_by_customer_flipped = spend_by_customer.map(lambda x: (x[1], x[0])).sortByKey()
results = spend_by_customer_flipped.collect()

# Print results
for result in results:
    print('Total spend:', result[0], f'(customer {result[1]})')



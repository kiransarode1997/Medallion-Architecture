import os
#
#  Declare Environment Variables
os.environ['envn'] = 'TEST'
os.environ['header'] = 'True'
os.environ['inferSchema'] = 'True'

envn = os.environ['envn']
header = os.environ['header']
inferSchema = os.environ['inferSchema']
appName = "Pyspark Demo App"

# envn = 'TEST'
# header = True
# inferSchema = True
# appName = "Pyspark Demo App"
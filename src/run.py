import sys
import os
import socket
from _thread import *
import threading

class MapReduce:

	def __init__(self):
		self.keys = {}
		self.values = []

	# Creates a master server to create worker threads
	def startSocket(N, udf):
		try:
			# Create a socket
			host = "127.0.0.1"
			port = 12345
			s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
			s.bind((host, port))

		    # Put the socket into listening mode
			s.listen(2 * N)
			
			# Loop until workers are all created
			for i in range(2*N):

				# Establish connection with client, save client for later
				c, addr = s.accept()

				# Start a new thread, keep track of it
				threading.Thread(target=MapReduce.threaded, args=[c, udf]).start()

		# Except it when Ctrl + C
		except KeyboardInterrupt:
			pass

	# Worker function, receives data from client socket
	def threaded(c, udf):
		# Data received from client
		data = c.recv(1048576)
		data = data.decode('utf-8')
		data = eval(data)

		# Check if map or reduce function is called
		maporReduce = data[-1]
		data = data[0:len(data) - 1]

		# Check for location of input/output file
		fileLocation = data[-1]
		data = data[0:len(data) - 1]
			
		# If map function is called, we execute the user defined map function
		if maporReduce == 'map':
			for x in data:
				udf.map(fileLocation, x)
		else:
			# Take in list of intermediate file locations
			values = data[-1]
			data = data[0:len(data) - 1]

			# Iterate through each key and call reduce
			for x in data:
				udf.reduce(x, values, fileLocation)

		# Close connection with the client
		c.close()
		return

	def createClient(data):
		# Creates socket connection, connects to local host server (master), sends the data, and closes
		s = socket.socket()
		s.connect(("127.0.0.1", 12345))
		s.send(data)
		s.close()

	# Function creates server (master) and uses client connections to create workers and send data for jobs
	def mainFunc(self, inputfileLocation, N, UDFfile, outputfileLocation):

		# Import UDF map and reduce functions
		sys.path.insert(0, UDFfile)
		module = UDFfile.split("/")[1]
		udf = __import__(module)
		
		# Start server socket (master)
		threading.Thread(target=MapReduce.startSocket, args = [N, udf]).start()
		
		# Read in data
		file = open(inputfileLocation)
		lines = file.readlines()
		
		# If more than one partition needs to be created; subsets the input list into equal sized partitions
		k, m = divmod(len(lines), N)
		partitioned =  list(lines[i*k+min(i, m):(i+1)*k+min(i+1, m)] for i in range(N))

		# Use partitions to create client-server connections to transmit data between master and workers
		for x in range(0, len(partitioned)):
			partitioned[x].append(inputfileLocation)
			partitioned[x].append('map')

			temp = str(partitioned[x])
			temp = temp.encode()

			threading.Thread(target=MapReduce.createClient, args=[temp]).start()

		# Create a global synchronization barrier, stopping reducer creation until mappers have produced all intermediate files
		directory = UDFfile + '/intermediates/'
		prevLoop = 0
		while True:
			numFiles = len(os.listdir(directory)) - 1
			if (numFiles == prevLoop):
				if (len(lines) == numFiles):
					break
				else:
					print("Fault Detected")
					
			prevLoop = numFiles

		# Determine the set of unique keys by parsing through intermediate files; unique keys then stored in dict "keys"
		for file in os.listdir(directory):
			if file.startswith('intermediate'):
				self.values.append(directory + file)
				intermediate = open(directory + file)
				words = intermediate.readlines()
				for i in words:
					j = (i.strip().split(' '))[0]
					if j in self.keys:
						pass
					else:
						self.keys[j] = 1
		self.keys = list(self.keys.keys())
		
		# Assign keys to reducers
		# If more than one partition needs to be created; subsets the input list into equal sized partitions
		k, m = divmod(len(self.keys), N)
		partitioned =  list(self.keys[i*k+min(i, m):(i+1)*k+min(i+1, m)] for i in range(N))

		# Use partitions to create client-server connections to transmit data between master and workers
		for x in range(0, len(partitioned)):
			partitioned[x].append(self.values)
			partitioned[x].append(outputfileLocation)
			partitioned[x].append('reduce')

			temp = str(partitioned[x])
			temp = temp.encode()
			threading.Thread(target=MapReduce.createClient, args=[temp]).start()
				
if __name__ == '__main__':
	# Gather arguments/configurations from function call, run UDF 
	mapReducer = MapReduce()
	config = sys.argv[1]
	udf = sys.argv[2]
	conf = {}
	with open(config) as fp:
		for line in fp:
				key, val = line.strip().split('=')
				conf[key.strip()] = val.strip().replace('"', '')
	
	mapReducer.mainFunc(conf['inputfileLocation'], int(conf['N']), udf, conf['outputfileLocation'])
import sys, random
import json
from pyspark import SparkContext, SparkConf, broadcast
import os
import hashlib
import mmh3

# function to create unique ids for each record
def create_id(rec):
  return rec['reviewerID']+'_'+ rec['asin']

# function to create 5-shingles for a string
def create_shingles(text):
  shingles = set()
  for i in range(len(text)-4):
    shingles.add(text[i:i+5])
  return shingles

if __name__ == "__main__":
  # create Spark context
  conf = SparkConf().setAppName("DuplicateReviewDetection")
  # sc = SparkContext(conf=conf)
  sc = SparkContext.getOrCreate(conf=conf)


  # get input filename from command line argument
  filename = sys.argv[1]
  # filename = 'Appliances_5.json'

  # read json file into RDD
  reviews_rdd = sc.textFile(filename).map(lambda x: json.loads(x))

  # assign unique ids to each record
  reviews_rdd = reviews_rdd.map(lambda rec: (create_id(rec), rec))

  # remove duplicate records
  reviews_rdd = reviews_rdd.reduceByKey(lambda x, y: x).map(lambda x: x[1])

  # replace missing reviewText with empty string
  reviews_rdd = reviews_rdd.map(lambda rec: (create_id(rec), rec.get('reviewText', '')))

  # create shingles for each review
  reviews_rdd = reviews_rdd.map(lambda x: (x[0], x[1], create_shingles(x[1])))
  num_shingles = len(reviews_rdd.reduce(lambda a,b: ("","", a[2].union(b[2])))[2])
  # print("Num shingles", num_shingles)

  # Checkpoint 2.1: Print the first 5 records.
  # save output to file
  with open('a2_p2_Narayanasamy_114941190_OUTPUT.txt', 'w') as file:
    file.write(f'Checkpoint 2.1: Print the first 5 records.\n\n')
    for rec in reviews_rdd.take(5):
      file.write(f"{rec}\n")
      print(rec)
      print("\n")
  

  # create broadcast variable for 1000 hash functions
  num_hashes = 1000  # number of hash functions to use
  hash_funcs = [hashlib.sha1((str(i)).encode('utf-8')).hexdigest() for i in range(num_hashes)]
  hash_funcs_broadcast = sc.broadcast(hash_funcs)

  # define function to create signature for each review
  def create_signature(rec):
      signature = []
      shingles = rec[2]
      for i in range(num_hashes):
          min_hash = float('inf')
          for shingle in shingles:
              # compute hash value for shingle using ith hash function
              hash_value = mmh3.hash(shingle, i)%num_shingles
              # update min_hash if necessary
              if hash_value < min_hash:
                  min_hash = hash_value
          signature.append(min_hash)
      return (rec[0], rec[1], signature)

  # create signature for each review
  reviews_rdd = reviews_rdd.map(create_signature)

  if filename == 'Appliances_5.json' : 

    # print signatures of selected records
    rec_id_1 = 'A34A1UP40713F8_B000NCTOUM'
    rec_1 = reviews_rdd.filter(lambda x: x[0] == rec_id_1).collect()[0]
    print("Checkpoint 2.2: Print the signatures of the following records:\n")
    with open('a2_p2_Narayanasamy_114941190_OUTPUT.txt', 'a') as file:
      file.write(f'\n\nCheckpoint 2.2: Print the signatures of the following records:\n\n')
      file.write(f"Signatures for {rec_id_1}: {rec_1[2]}\n")
    print(f'Signatures for {rec_id_1}: {rec_1[2]}')

    rec_id_2 = 'A3LGZ8M29PBNGG_B000W3P4AQ'
    rec_2 = reviews_rdd.filter(lambda x: x[0] == rec_id_2).collect()[0]
    with open('a2_p2_Narayanasamy_114941190_OUTPUT.txt', 'a') as file:
      # file.write(f'\n\nCheckpoint 2.2: Print the signatures of the following records:\n\n')
      file.write(f"Signatures for {rec_id_2}: {rec_2[2]}\n")
    print(f'Signatures for {rec_id_2}: {rec_2[2]}')

    rec_id_3 = 'AFUVGAUNQVT0S_B004XLDE5A'
    rec_3 = reviews_rdd.filter(lambda x: x[0] == rec_id_3).collect()[0]
    with open('a2_p2_Narayanasamy_114941190_OUTPUT.txt', 'a') as file:
      # file.write(f'\n\nCheckpoint 2.2: Print the signatures of the following records:\n\n')
      file.write(f"Signatures for {rec_id_3}: {rec_3[2]}\n")
    print(f'Signatures for {rec_id_3}: {rec_3[2]}')
  
  else:

    # print signatures of selected records
    rec_id_1 = 'A26S0R5B6D0BP9_B00006LVF1'
    rec_1 = reviews_rdd.filter(lambda x: x[0] == rec_id_1).collect()[0]
    print("Checkpoint 2.2: Print the signatures of the following records:\n")
    with open('a2_p2_Narayanasamy_114941190_OUTPUT.txt', 'a') as file:
      file.write(f'\n\nCheckpoint 2.2: Print the signatures of the following records:\n\n')
      file.write(f"Signatures for {rec_id_1}: {rec_1[2]}\n")
    print(f'Signatures for {rec_id_1}: {rec_1[2]}')

    rec_id_2 = 'A3SS6919NRQ2MF_B00006I5SA'
    rec_2 = reviews_rdd.filter(lambda x: x[0] == rec_id_2).collect()[0]
    with open('a2_p2_Narayanasamy_114941190_OUTPUT.txt', 'a') as file:
      # file.write(f'\n\nCheckpoint 2.2: Print the signatures of the following records:\n\n')
      file.write(f"Signatures for {rec_id_2}: {rec_2[2]}\n")
    print(f'Signatures for {rec_id_2}: {rec_2[2]}')

    rec_id_3 = 'AYM76JWI220Z4_B0000WR5EW'
    rec_3 = reviews_rdd.filter(lambda x: x[0] == rec_id_3).collect()[0]
    with open('a2_p2_Narayanasamy_114941190_OUTPUT.txt', 'a') as file:
      # file.write(f'\n\nCheckpoint 2.2: Print the signatures of the following records:\n\n')
      file.write(f"Signatures for {rec_id_3}: {rec_3[2]}\n")
    print(f'Signatures for {rec_id_3}: {rec_3[2]}')

  # Step 1: Collect the signatures
  signatures = reviews_rdd.map(lambda x: (x[0], x[2]))

  # Define the target IDs based on the filename
  if filename == 'Appliances_5.json':
      targetIDs = ["A34A1UP40713F8_B000NCTOUM", "A3LGZ8M29PBNGG_B000W3P4AQ", "AFUVGAUNQVT0S_B004XLDE5A"]
  else:
      targetIDs = ["A26S0R5B6D0BP9_B00006LVF1", "A3SS6919NRQ2MF_B00006I5SA", "AYM76JWI220Z4_B0000WR5EW"]

  # Map signatures and create a map of targets' signatures
  signature_map = signatures.collectAsMap()

  targets = {id: signature_map[id] for id in targetIDs}
  for id in targetIDs:
      del signature_map[id]

  # Step 2: Split the signatures into bands
  band_numbers = 200
  row_numbers = 5
  bucket_numbers = 11

  def split_bands(rec):
      (id, signature) = rec
      bands = []
      for b in range(band_numbers):
          bands.append((id, b, signature[b*row_numbers:(b+1)*row_numbers]))
      return bands

  candidate_signature = sc.parallelize(list(signature_map.items()))
  target_signature = sc.parallelize(list(targets.items()))

  candidate_bands = candidate_signature.flatMap(split_bands)
  target_bands = target_signature.flatMap(split_bands)

  # Step 3: Hash the bands and get candidate pairs
  prime = 57
  mod = 5987774053

  def locality_sensitive_hashing(band):
    (id, bandId, signtre) = band
    hash_value = sum([hash(str(bandId) + '_' + str(x)) for x in signtre])
    return ((bandId, hash_value % bucket_numbers), id)


  candidate_buckets = candidate_bands.map(locality_sensitive_hashing)
  target_buckets = target_bands.map(locality_sensitive_hashing)

  candidate_pairs = target_buckets.join(candidate_buckets).map(lambda rec: rec[1]).distinct()

  # Step 4: Compute Jaccard similarity for candidate pairs and filter out dissimilar ones
  num_hashes = band_numbers * row_numbers

  def jaccard_similarity(pair):
      (target_id, candidate_id) = pair
      target_sig = targets[target_id] 
      candidate_sig = signature_map[candidate_id]
      count = 0
      for target, candidate in zip(target_sig, candidate_sig):
          if target == candidate:
              count += 1
      return (pair, count/num_hashes)

  similar_pairs = candidate_pairs.map(jaccard_similarity).filter(lambda pair: pair[1] >= 0.8)
  similar_pairs_dict = similar_pairs.collectAsMap()
  # print(similar_pairs.collect())

  print("Checkpoint 2.3: For each target record, list the first 10 records found that have JSim >=0.80.\n")
  with open('a2_p2_Narayanasamy_114941190_OUTPUT.txt', 'a') as file:
      file.write(f'\n\nCheckpoint 2.3: For each target record, list the first 10 records found that have JSim >=0.80.\n\n')
  for target_id in targetIDs:
    with open('a2_p2_Narayanasamy_114941190_OUTPUT.txt', 'a') as file:
      file.write(f"Results for target ID: {target_id}\n\n")
    print("Results for target ID:", target_id)
    count = 0
    for pair, jsim in similar_pairs_dict.items():
        target, candidate = pair
        if target == target_id and jsim >= 0.8:
            with open('a2_p2_Narayanasamy_114941190_OUTPUT.txt', 'a') as file:
            # file.write(f'\n\nCheckpoint 2.3: Print the signatures of the following records:\n\n')
              file.write(f"Jaccard Similarity: {jsim}, Candidate ID: {candidate}\n")
            print("JSim:", jsim, "Candidate ID:", candidate)
            count += 1
            if count == 10:
                break
    with open('a2_p2_Narayanasamy_114941190_OUTPUT.txt', 'a') as file:
            # file.write(f'\n\nCheckpoint 2.3: Print the signatures of the following records:\n\n')
      file.write(f"\n\n")        
    print("\n")


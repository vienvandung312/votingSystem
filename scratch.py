from db import SingletonDBConnection

s1 = SingletonDBConnection(host='postgres', dbname="voting", user="postgres", password="postgres")
s2 = SingletonDBConnection(host='potgres', dbname="voting", user="postgres", password="postgres")

if id(s1) == id(s2):
    print("Singleton works, both variables contain the same instance.")
else:
    print("Singleton failed, variables contain different instances.")


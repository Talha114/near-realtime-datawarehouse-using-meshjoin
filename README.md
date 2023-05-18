# near-realtime-datawarehouse-using-meshjoin

## Project Overview: ##
In this Project, I Implemented a Near Real-time Data Warehouse Prototype for METRO. To mimic the near real-time Data Warehouse using 10,000 Transactions from METRO Against 100 products present in Master Data. The schema for this project is star schema

![Screenshot (109)](https://user-images.githubusercontent.com/88310782/229909151-f3cc8c5f-2880-4fef-b927-e6f7d88f5383.png)


## Drawback of meshjoin:
Meshjoin needs to be modified if we are dealing with two tables in master data while maintaining only one hashmap and queue for both master data

Procedure MeshJoin()
	for  numTransaction do
		Queue <- Transaction
		For Queue do
			HashTable<- <product_id,transaction>
			Diskbuffer1<-product
			Diskbuffer2<-customer
			if HashTable(diskbuffer1.product_id) = diskbuffer1.product_id then
				Enrich(Transaction.tuple)
				loadTuple Transaction.tuple
			end if
			
			if Queue is Full then
				removeFirstElement
			end if
		end for
	end for


We learned about the process of ETL on a basic level. How data is transformed and summarized to be stored in data warehouse.

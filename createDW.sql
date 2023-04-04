CREATE DATABASE  IF NOT EXISTS `Metro_dwh`;
USE `Metro_dwh`;

DROP TABLE IF EXISTS `transaction_fact`;

--
-- table `customer`
--

DROP TABLE IF EXISTS `dim_customer`;
CREATE TABLE `dim_customer` (
  `customer_id` varchar(4) NOT NULL,
  `customer_name` varchar(30) NOT NULL,
  PRIMARY KEY (`customer_id`)
);

--
-- table `date`
--

DROP TABLE IF EXISTS `dim_date`;
CREATE TABLE `dim_date` (
  `date` date NOT NULL,
  `day` varchar(10) NOT NULL,
  `month` int NOT NULL,
  `quarter` int NOT NULL,
  `year` int NOT NULL,
  PRIMARY KEY (`date`)
);

--
-- table `product`
--

DROP TABLE IF EXISTS `dim_product`;
CREATE TABLE `dim_product` (
  `product_id` varchar(6) NOT NULL,
  `product_name` varchar(30) NOT NULL,
  PRIMARY KEY (`product_id`)
);

--
-- table `store`
--

DROP TABLE IF EXISTS `dim_store`;
CREATE TABLE `dim_store` (
  `store_id` varchar(4) NOT NULL,
  `store_name` varchar(20) NOT NULL,
  PRIMARY KEY (`store_id`)
);

--
-- table `supplier`
--
DROP TABLE IF EXISTS `supplier`;
DROP TABLE IF EXISTS `dim_supplier`;
CREATE TABLE `dim_supplier` (
  `supplier_id` varchar(5) NOT NULL,
  `supplier_name` varchar(30) NOT NULL,
  PRIMARY KEY (`supplier_id`)
);
--
-- table `transaction_fact`
--


CREATE TABLE `transaction_fact` (
  `transaction_id` int NOT NULL,
  `product_id` varchar(6) NOT NULL,
  `customer_id` varchar(4) NOT NULL,
  `store_id` varchar(4) NOT NULL,
  `date_id` date NOT NULL,
  `supplier_id` varchar(5) NOT NULL,
  `quantity` smallint NOT NULL,
  `total_sale` decimal(7,2) NOT NULL,
  PRIMARY KEY (`transaction_id`),
  KEY `customer_idx` (`customer_id`),
  KEY `store_idx` (`store_id`),
  KEY `date_idx` (`date_id`),
  KEY `product` (`product_id`),
  KEY `supplier_idx` (`supplier_id`),
  CONSTRAINT `customer` FOREIGN KEY (`customer_id`) REFERENCES `dim_customer` (`customer_id`),
  CONSTRAINT `date` FOREIGN KEY (`date_id`) REFERENCES `dim_date` (`date`),
  CONSTRAINT `product` FOREIGN KEY (`product_id`) REFERENCES `dim_product` (`product_id`),
  CONSTRAINT `store` FOREIGN KEY (`store_id`) REFERENCES `dim_store` (`store_id`),
  CONSTRAINT `supplier` FOREIGN KEY (`supplier_id`) REFERENCES `dim_supplier` (`supplier_id`)
);
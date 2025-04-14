CREATE SCHEMA IF NOT EXISTS sales_data;
USE sales_data;

CREATE TABLE IF NOT EXISTS sales (
SalesID INT PRIMARY KEY,
SalesPersonID INT,
CustomerID INT,
ProductID INT,
Quantity INT,
Discount NUMERIC,
TotalPrice DECIMAL(10,2),
SalesDate DATETIME, 
TransactionNumber VARCHAR(255));

CREATE TABLE IF NOT EXISTS categories (
CategoryID INT PRIMARY KEY,
CategoryName VARCHAR(45));

CREATE TABLE IF NOT EXISTS cites(
CityID INT PRIMARY KEY,
CityName VARCHAR(45),
Zipcode NUMERIC(5),
CountryID INTEGER REFERENCES countries (CountryID) ON DELETE RESTRICT);

CREATE TABLE IF NOT EXISTS countries(
CountryID INT PRIMARY KEY,
CountryName VARCHAR(45),
CountryCode VARCHAR(2));

CREATE TABLE IF NOT EXISTS customers(
CustomerID INT PRIMARY KEY,
FirstName VARCHAR(45),
MiddleInitial VARCHAR(1),
LastName VARCHAR(45),
CityID INT REFERENCES cites (CityID) ON DELETE RESTRICT,
Address VARCHAR(90));

CREATE TABLE IF NOT EXISTS employees(
EmployeeID INT PRIMARY KEY,
FirstName VARCHAR(45),
MiddleInitial VARCHAR(1),
LastName VARCHAR(45),
BirthDate DATE,
Gender VARCHAR(1),
CityID INT REFERENCES Cites (CityID) ON DELETE RESTRICT,
HireDate DATE);

CREATE TABLE IF NOT EXISTS products(
ProductID INT PRIMARY KEY,
ProductName VARCHAR(45),
Price DECIMAL,
CategoryID INT REFERENCES categories (CategoryID) ON DELETE RESTRICT, 
ModifyDate DATE,
Resistant VARCHAR(45),
IsAllergic VARCHAR(10) CHECK (IsAllergic IN ('True','False','Unknown')), 
VitalityDays NUMERIC(3));
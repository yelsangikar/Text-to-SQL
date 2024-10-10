from dotenv import load_dotenv
load_dotenv()
import streamlit as st
import time
import os
import google.generativeai as genai
import pandas as pd
import pyodbc
import warnings
warnings.filterwarnings('ignore')

# Configure the Google Generative AI API
genai.configure(api_key=os.getenv('GOOGLE_API_KEY'))

def get_gemini_response(question, prompt):
    model = genai.GenerativeModel('gemini-pro')
    response = model.generate_content([prompt[0], question])
    return response.text

def execute_sql_query(sql_query, server, database):
    try:
        # Create the connection string
        connection = pyodbc.connect(driver='ODBC Driver 17 for SQL Server',
                                    server=server,
                                    Timeout=30,
                                    database=database,
                                    trusted_connection='yes')

        # Create a cursor from the connection
        cur = connection.cursor()

        # Execute the SQL query
        cur.execute(sql_query)

        # Check if the query is a SELECT statement
        if sql_query.strip().upper().startswith("SELECT"):
            # Fetch all the results
            rows = cur.fetchall()

            # Get column names from the cursor
            columns = [column[0] for column in cur.description]

            # Close the connection
            connection.close()

            # Convert the results into a DataFrame
            df = pd.DataFrame.from_records(rows, columns=columns)
            return df, rows, None  # No error, return None for error message
        else:
            # For non-SELECT queries (like UPDATE, INSERT, DELETE)
            connection.commit()
            connection.close()
            return None, None, "Query executed successfully but no results to display."

    except Exception as e:
        return None, None, str(e)  # Return the error message if SQL fails


def correct_sql_query(question, response, prompt):
    error = True
    corrected_query = response
    schema_info = f"""
        The relevant table schema is as follows:
        **Products table**:
        - ProductID (int)
        - ProductName (nvarchar)
        - SupplierID (int)
        - CategoryID (int)
        - QuantityPerUnit (nvarchar)
        - UnitPrice (decimal)
        - UnitsInStock (int)
        - UnitsOnOrder (int)
        - ReorderLevel (int)
        - Discontinued (bit)

        **Suppliers table**:
        - SupplierID (int)
        - CompanyName (nvarchar)
        - ContactName (nvarchar)
        - ContactTitle (nvarchar)
        - Address (nvarchar)
        - City (nvarchar)
        - Region (nvarchar)
        - PostalCode (nvarchar)
        - Country (nvarchar)
        - Phone (nvarchar)
        - Fax (nvarchar)
        - HomePage (nvarchar)
        """

    while error:
        # Execute the SQL query and check if it fails
        query_result, rows, error_message = execute_sql_query(corrected_query,  os.getenv('server'), 'master')

        if error_message and "no results" not in error_message:
            # Display the SQL error
            st.error(f"SQL Error: {error_message}")

            # Create a correction prompt based on the error and provide the schema
            correction_prompt = f"""
            The following SQL query resulted in an error: "{corrected_query}". 
            The error was: "{error_message}". 
            Please correct the query based on this error. 
            {schema_info}
            Make sure to alias columns if there are duplicates.
            """

            corrected_query = get_gemini_response(question, [correction_prompt])
            st.write(f"Corrected SQL Query: {corrected_query}")

        elif error_message and "no results" in error_message:
            st.warning(f"Warning: {error_message}")
            return None, None, error_message

        else:
            return query_result, rows, None  # No error, successful execution


def get_detailed_ai_response(question, query_result):
    """
    Uses AI to generate a more detailed natural language response based on the SQL query result.
    """
    sql_response_str = "\n".join([", ".join(map(str, row)) for row in query_result])

    # AI prompt for generating a detailed response
    detailed_response_prompt = f"""
     You are an expert at converting SQL query results into detailed natural language responses. Based on the following SQL query result:

     {sql_response_str}

     The user asked: "{question}"

     Please provide a detailed response that answers the question clearly and includes additional relevant information.    
     **Database Schema Overview**:
     - Customers table (CustomerID, CompanyName, ContactName, ContactTitle, Address, City, Region, PostalCode, Country, Phone, Fax)
     - Suppliers table (SupplierID, CompanyName, ContactName, ContactTitle, Address, City, Region, PostalCode, Country, Phone, Fax, HomePage)
     - Products table (ProductID, ProductName, SupplierID, CategoryID, QuantityPerUnit, UnitPrice (in rupees), UnitsInStock, UnitsOnOrder, ReorderLevel, Discontinued)

     This should include:
     - Key findings from the SQL query results (e.g., relevant customer details).
     - Any names of customers, their companies, and other pertinent information, if applicable.
     - Context or explanations that enhance the user's understanding of the data.
     - Any trends or patterns evident in the customer data that might be of interest (e.g., distribution by city, country, etc.).

     Make sure the response is structured, informative, and easy to understand.
     """

    # AI generates the detailed natural language response based on the SQL result and user question
    model = genai.GenerativeModel('gemini-pro')
    response = model.generate_content([detailed_response_prompt])

    return response.text

prompt = [
    """
    You are an advanced SQL generator for Microsoft SQL Server. Your task is to convert English questions into accurate SQL commands based on the database named master, which contains the following tables and columns:

    **Customers table**:
    - CustomerID (nchar)
    - CompanyName (nvarchar)
    - ContactName (nvarchar)
    - ContactTitle (nvarchar)
    - Address (nvarchar)
    - City (nvarchar)
    - Region (nvarchar)
    - PostalCode (nvarchar)
    - Country (nvarchar)
    - Phone (nvarchar)
    - Fax (nvarchar)

    **Suppliers table**:
    - SupplierID (int)
    - CompanyName (nvarchar)
    - ContactName (nvarchar)
    - ContactTitle (nvarchar)
    - Address (nvarchar)
    - City (nvarchar)
    - Region (nvarchar)
    - PostalCode (nvarchar)
    - Country (nvarchar)
    - Phone (nvarchar)
    - Fax (nvarchar)
    - HomePage (nvarchar)

    **Products table**:
    - ProductID (int)
    - ProductName (nvarchar)
    - SupplierID (int)
    - CategoryID (int)
    - QuantityPerUnit (nvarchar)
    - UnitPrice (decimal)
    - UnitsInStock (int)
    - UnitsOnOrder (int)
    - ReorderLevel (int)
    - Discontinued (bit)

    **Important Rules**:
    - **Do NOT** include any formatting characters, such as backticks (`), triple quotes (\"\"\"), or code blocks (```).
    - **Do NOT** output any text like "```sql" or any formatting instructions. The SQL query should be **directly executable** in a standard SQL environment.
    - **Only use standard SQL syntax** without extra formatting.
    - **Keywords** like SELECT, FROM, WHERE, GROUP BY, etc., should be **capitalized**.
    - **String values** should be enclosed in single quotes (').
    - **Do NOT** include the word "SQL" or any non-SQL content in your response.

    Here are some examples of the correct format:

    Example 1 - How many suppliers are there in the database?
    Expected SQL command:
    SELECT COUNT(*) FROM Suppliers;

    Example 2 - List all products that are discontinued.
    Expected SQL command:
    SELECT * FROM Products WHERE Discontinued = 1;

    Example 3 - Retrieve the names of suppliers and their cities.
    Expected SQL command:
    SELECT CompanyName, City FROM Suppliers;

    Example 4 - Update the unit price for 'Chai' to 18.50.
    Expected SQL command:
    UPDATE Products SET UnitPrice = 18.50 WHERE ProductName = 'Chai';

    Example 5 - List all suppliers in 'Germany'.
    Expected SQL command:
    SELECT * FROM Suppliers WHERE Country = 'Germany';

    Example 6 - Retrieve product names and the corresponding supplier names.
    Expected SQL command:
    SELECT Products.ProductName, Suppliers.CompanyName 
    FROM Products 
    INNER JOIN Suppliers ON Products.SupplierID = Suppliers.SupplierID;

    Example 7 - Find all products supplied by 'Exotic Liquids'.
    Expected SQL command:
    SELECT Products.ProductName 
    FROM Products 
    INNER JOIN Suppliers ON Products.SupplierID = Suppliers.SupplierID 
    WHERE Suppliers.CompanyName = 'Exotic Liquids';

    Example 8 - Get the total units in stock for each supplier.
    Expected SQL command:
    SELECT Suppliers.CompanyName, SUM(Products.UnitsInStock) AS TotalStock 
    FROM Suppliers 
    INNER JOIN Products ON Suppliers.SupplierID = Products.SupplierID 
    GROUP BY Suppliers.CompanyName;

    Example 9 - List all suppliers and the number of products they supply.
    Expected SQL command:
    SELECT Suppliers.CompanyName, COUNT(Products.ProductID) AS ProductCount 
    FROM Suppliers 
    LEFT JOIN Products ON Suppliers.SupplierID = Products.SupplierID 
    GROUP BY Suppliers.CompanyName;

    Example 10 - List products and their suppliers, but only for products with less than 20 units in stock.
    Expected SQL command:
    SELECT Products.ProductName, Suppliers.CompanyName 
    FROM Products 
    INNER JOIN Suppliers ON Products.SupplierID = Suppliers.SupplierID 
    WHERE Products.UnitsInStock < 20;

    Example 11 - Find all suppliers who don't supply any products.
    Expected SQL command:
    SELECT Suppliers.CompanyName 
    FROM Suppliers 
    LEFT JOIN Products ON Suppliers.SupplierID = Products.SupplierID 
    WHERE Products.ProductID IS NULL;

    If the question cannot be converted into a valid SQL command, respond with: "I'm sorry, I cannot generate a SQL command for that question."
    """
]

# Set page config with a custom icon, layout, and styling
st.set_page_config(page_title='Text-to-SQL', layout="centered", page_icon=":bar_chart:")
st.markdown(
    """
    <style>
    body {
        background-image: url('https://www.transparenttextures.com/patterns/shattered.png');
        background-size: cover;
    }
    h1, h3 {
        text-align: center;
    }
    h1 {
        color: #4CAF50;
    }
    h3 {
        color: #ff6f61;
        text-shadow: 1px 1px 2px black;
    }
    </style>
    <h1>⭐ Text to SQL ⭐ <br> <small>Retrieve SQL Data Seamlessly</small></h1>
    <h3>Your SQL Data at Your Fingertips!</h3>
    """,
    unsafe_allow_html=True
)

# Input and button
question = st.text_input('Enter your SQL query here:', key='input', placeholder='SELECT * FROM your_table WHERE ...', help='E.g., Retrieve specific rows, filter results, or join tables')
submit = st.button('Ask The Question', key='submit', help='Click to run your SQL query!', use_container_width=True)


# Display loading spinner while processing the query
if submit:
    with st.spinner('Processing your query...'):
        time.sleep(2)  # Simulating query processing delay
        st.success('Query executed successfully! :rocket:')

# Enhanced button CSS
st.markdown(
    """
    <style>
    div.stButton > button {
        background: linear-gradient(to right, #ff7e5f, #feb47b);
        color: white;
        padding: 10px;
        font-size: 16px;
        border-radius: 10px;
        transition: 0.3s;
    }
    div.stButton > button:hover {
        background: linear-gradient(to right, #feb47b, #ff7e5f);
        transform: scale(1.05);
        box-shadow: 0px 4px 15px rgba(0, 0, 0, 0.1);
    }
    </style>
    """,
    unsafe_allow_html=True
)

# Adding a footer for credits or links
st.markdown(
    """
    <hr>
    <footer style="text-align: center; color: grey;">
        <p>Made by : Genworks Digital Team | <a href="https://yourwebsite.com" style="color: #ff6f61;">Visit Us</a></p>
    </footer>
    """,
    unsafe_allow_html=True
)


# Streamlit logic
if submit:
    # Get the SQL query from the AI model
    response = get_gemini_response(question, prompt)
    st.write(f"Generated SQL Query: {response}")

    # Loop to correct the SQL query until it is correct
    query_result, rows, error_message = correct_sql_query(question, response, prompt)


    if error_message:
        if "no results" in error_message:
            st.warning(f"{error_message}")
        else:
            st.error(f"SQL Error after correction: {error_message}")
    else:
        st.subheader('Query Result:')
        st.table(query_result)

        # Generate detailed AI response
        detailed_response = get_detailed_ai_response(question, rows)
        st.subheader('Detailed Natural Language Response:')
        st.write(detailed_response)

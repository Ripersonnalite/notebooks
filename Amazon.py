from bs4 import BeautifulSoup
import requests
import pandas as pd
import numpy as np
import Gmail as g

# Function to extract Product Title
def get_title(soup):

    try:
        # Outer Tag Object
        title = soup.find("span", attrs={"id":'productTitle'})
        
        # Inner NavigatableString Object
        title_value = title.text

        # Title as a string value
        title_string = title_value.strip()

    except AttributeError:
        title_string = ""

    return title_string

# Function to extract Product Price
def get_price(soup):
    
    try:
        #price = soup.find("span", attrs={'id':'priceblock_ourprice'}).string.strip()
        price = soup.find_all("span")

        for i in price:
            try:
                if i['class'] == ['a-price-whole']:
                    price = f"${str(i.get_text())[:-1]}"
                    return price
            except KeyError:
                continue 
    except AttributeError:
                    
        try:
            # If there is some deal price
            price = soup.find("span", attrs={'id':'priceblock_dealprice'}).string.strip()

        except:
            price = ""
    
    return price


def get_price_jp(soup):

    try:
        price = soup.find("span", attrs={'a-price-whole'}).string.strip()

    except AttributeError:

        try:
            # If there is some deal price
            price = soup.find("span", attrs={'id':'a-price-whole'}).string.strip()

        except:
            price = ""

    return price

# Function to extract Product Rating
def get_rating(soup):

    try:
        rating = soup.find("i", attrs={'class':'a-icon a-icon-star a-star-4-5'}).string.strip()
    
    except AttributeError:
        try:
            rating = soup.find("span", attrs={'class':'a-icon-alt'}).string.strip()
        except:
            rating = ""	

    return rating

# Function to extract Number of User Reviews
def get_review_count(soup):
    try:
        review_count = soup.find("span", attrs={'id':'acrCustomerReviewText'}).string.strip()

    except AttributeError:
        review_count = ""	

    return review_count

# Function to extract Availability Status
def get_availability(soup):
    try:
        available = soup.find("div", attrs={'id':'availability'})
        available = available.find("span").string.strip()

    except AttributeError:
        available = "Not Available"	

    return available

def clear_file(file_in):
    """By Ricardo Kazuo"""
    try:
        file_to_delete = open(file_in,'w')
        file_to_delete.close()
    except:
        pass

def fetch_all():
    data = pd.DataFrame()
    clear_file("amazon_data.csv")
    # add your user agent 
    HEADERS = ({'User-Agent':'', 'Accept-Language': 'en-US, en;q=0.5'})
    
    products = {"Airpods Pro","Apple Imac","Apple Mac Mini","Apple Mac Studio","Apple Macbook Air","Apple Watch Se",
    "Apple Watch Series 9","Apple Watch Ultra 2","Gameboy","Ipad Pro","Iphone 14 Pro Max","Iphone 15 Pro Max","Mackbook Pro",
    "Mega Drive","Nintendo Switch","Ps4","Ps5","Airpods Max","Psp","Xbox Series X","Product"}
    
    #products = {"Airpods Pro"}
    
    address = {
    "https://www.amazon.ca/s?k=",
    "https://www.amazon.fr/s?k=",
    "https://www.amazon.de/s?k=",
    "https://www.amazon.co.jp/s?k=",
    "https://www.amazon.co.uk/s?k="
    }  
    
    for product in products:
        print("---------------->"+product)
        for addres in address:
            print("---------------->"+addres)
            try:
                # HTTP Request
                webpage = requests.get(addres+product, headers=HEADERS)
                # Soup Object containing all data
                soup = BeautifulSoup(webpage.content, "html.parser")

                # Fetch links as List of Tag Objects
                links = soup.find_all("a", attrs={'class':'a-link-normal s-no-outline'})

                # Store the links
                links_list = []

                # Loop for extracting links from Tag Objects
                for link in links:
                    links_list.append(link.get('href'))

                d = {"title":[], "price":[], "rating":[], "reviews":[],"availability":[]}
                url = addres.partition('/s')[0]

                # Loop for extracting product details from each link 
                for link in links_list:
                    new_webpage = requests.get( url + link, headers=HEADERS)

                    new_soup = BeautifulSoup(new_webpage.content, "html.parser")

                    # Function calls to display all necessary product information
                    d['title'].append(get_title(new_soup))
                    #print(d['title'])
                    if "jp" not in url:
                        d['price'].append(get_price(new_soup))
                    else:
                        d['price'].append(get_price_jp(new_soup))
                    d['rating'].append(get_rating(new_soup))
                    d['reviews'].append(get_review_count(new_soup))
                    d['availability'].append(get_availability(new_soup))
                    amazon_df = pd.DataFrame.from_dict(d)
                    amazon_df['title'].replace('', np.nan, inplace=True)
                    amazon_df = amazon_df.dropna(subset=['title'])
                    amazon_df.insert(0, "address", addres+product, True)
                    amazon_df2 = amazon_df
                    amazon_df=amazon_df[amazon_df["address"].str.contains(product)]
                    amazon_df = amazon_df[amazon_df["address"].str.contains("<") == False]
                    amazon_df = amazon_df[amazon_df["title"].str.contains("<") == False]
                    amazon_df = amazon_df[amazon_df["price"].str.contains("<") == False]
                    amazon_df = amazon_df[amazon_df["rating"].str.contains("<") == False]
                    amazon_df = amazon_df[amazon_df["reviews"].str.contains("<") == False]
                    amazon_df = amazon_df[amazon_df["availability"].str.contains("<") == False]
                print(amazon_df)
                amazon_df.to_csv("amazon_data.csv", header=True, mode="a", index=False)
                #amazon_df2.to_csv("amazon_data2.csv", header=True, mode="a", index=False)
            except:
                pass     
        #print(data)  
    
if __name__ == '__main__':
    fetch_all()

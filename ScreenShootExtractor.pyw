import time
import re
import os
import pickle
import shutil
import base64
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from webdriver_manager.chrome import ChromeDriverManager
from PIL import Image, ImageDraw, ImageFont
from bs4 import BeautifulSoup
from datetime import datetime

def capture_screenshot(url, filename_base, username, password, save_folder):
    # Generate filenames for both PNG and JPG
    png_path = os.path.join(save_folder, f"{filename_base}.png")
    jpg_path = os.path.join(save_folder, f"{filename_base}.jpg")

    chrome_options = Options()
    # chrome_options.add_argument("--headless")  # Optionally run in headless mode

    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=chrome_options)
    driver.set_window_size(1980, 1080)
    driver.get(url)

    time.sleep(5)  # Wait for the page to load

    # Login
    username_field = driver.find_element(By.ID, "j_username")
    password_field = driver.find_element(By.ID, "j_password")

    username_field.send_keys(username)
    password_field.send_keys(password)

    login_button = driver.find_element(By.ID, "btn_enter")
    login_button.click()

    time.sleep(5)  # Wait for the page to load after login
    driver.execute_script("document.body.style.zoom='40%'")
    time.sleep(15)

    # Extract date and time from HTML
    html = driver.page_source
    soup = BeautifulSoup(html, 'html.parser')
    # Find the first element with title="time"
    first_element = soup.find('div', title="time")
    date_time = first_element.find('span').text
    # Convert the date/time string to a datetime object
    date_time = datetime.strptime(date_time, "%m/%d/%Y, %I:%M:%S %p")  
    # Format the datetime object as dd/MM/yyyy
    formatted_date = date_time.strftime("%d/%m/%Y, %I:%M:%S %p")
    #print(f"Formatted Date: {formatted_date}")
    
    #print(BeautifulSoup(html, 'html.parser'))
    # Find all text content (including whitespace)
    #all_text = soup.get_text(strip=True)
    #all_text = []
    #for element in soup.findAll(True):
      # Check if element has text
      #if element.string:
      #  all_text.append(element.string.strip())

    # Find all elements with IDs
    #elements_with_ids = soup.find_all(True, id=True)
    # Print the IDs
    #for element in elements_with_ids:
    #    print(element['id'] + " " +element.text)
    #exit()
    
    
    # Save as PNG
    driver.save_screenshot(png_path)
    # Save screenshot as high-quality JPEG (adjust quality as needed)
    img = Image.open(png_path)
    img.save(jpg_path, quality=90)  # 90 is a good balance
    # Copy PNG to JPG
    shutil.copy(png_path, jpg_path)

    driver.quit()

    print(f"Screenshots captured and saved as PNG and JPG")

    # Convert images to base64
    png_base64 = convert_to_base64(png_path)
    jpg_base64 = convert_to_base64(jpg_path)

    # Save Base64 to a text file
    save_base64_to_file(save_folder, filename_base, png_base64, jpg_base64)

    # Split and save JPG parts
    split_and_save_jpg2(jpg_path, save_folder, filename_base, str(formatted_date), soup)
    
    return png_base64, jpg_base64

def convert_to_base64(file_path):
    with open(file_path, "rb") as image_file:
        return base64.b64encode(image_file.read()).decode('utf-8')

def save_base64_to_file(save_folder, filename_base, png_base64, jpg_base64):
    with open(os.path.join(save_folder, f"{filename_base}_base64.txt"), "w") as f:
        f.write(f"PNG Base64:\n{png_base64}\n\n")
        f.write(f"JPG Base64:\n{jpg_base64}\n")
    print(f"Base64 data saved to {filename_base}_base64.txt")

def delete_previous_files(save_folder, base_filename):
    for filename in os.listdir(save_folder):
        if filename.startswith(base_filename):
            file_path = os.path.join(save_folder, filename)
            os.remove(file_path)
            print(f"Deleted previous file: {filename}")

def split_and_save_jpg2(jpg_path, save_folder, filename_base, date_time, soup):
    with Image.open(jpg_path) as img:                        
        print(f"Date and Time: {date_time}")
        width, height = img.size
        # Define a dark orange color tuple (R, G, B)
        dark_orange = (255, 165, 0)
        
        # Calculate part sizes
        part1_width = int(width * 0.415)
        part2_width = int(width * 0.273)
        part3_width = width - part1_width - part2_width

        # Calculate new height (80% of original)
        new_height = int(height * 1)

        # Split the image
        part1 = img.crop((0, 0, part1_width, new_height))
        part2 = img.crop((part1_width, 0, part1_width + part2_width, new_height))
        part3 = img.crop((part1_width + part2_width, 0, width, new_height))

        # Further slice part1 on the left side by 15%
        part1_slice_width = int(part1_width * 0.15)
        part1_sliced = part1.crop((part1_slice_width, 0, part1_width, new_height))

        # Find the largest dimensions
        max_width = max(part1_sliced.width, part2.width, part3.width)
        max_height = new_height

        # Resize all parts to the largest dimensions
        part1_resized = part1_sliced.resize((max_width, max_height), Image.LANCZOS)
        part2_resized = part2.resize((max_width, max_height), Image.LANCZOS)
        part3_resized = part3.resize((max_width, max_height), Image.LANCZOS)

        # Add watermark to part2 and part3
        font_size = 20
        try:
            # Try to use Arial font (common on Windows)
            font = ImageFont.truetype("arial.ttf", font_size)
        except IOError:
            # If Arial is not available, use default font
            font = ImageFont.load_default()

        for part in [part2_resized, part3_resized]:
            draw = ImageDraw.Draw(part)
            draw.text((10, 130), "P74", font=font, fill=dark_orange)  # Black color with 50% opacity
            draw.text((10, 237), "P75", font=font, fill=dark_orange)  # Black color with 50% opacity
            draw.text((10, 343), "P76", font=font, fill=dark_orange)  # Black color with 50% opacity
            draw.text((10, 450), "P77", font=font, fill=dark_orange)  # Black color with 50% opacity
            draw.text((10, 557), "FPAB", font=font, fill=dark_orange)  # Black color with 50% opacity

        draw = ImageDraw.Draw(part1_resized)
        draw.text((250, 57), "Produção de Óleo", font=font, fill=dark_orange)
        draw.text((250, 657), date_time, font=font, fill=dark_orange)
        draw = ImageDraw.Draw(part2_resized)
        draw.text((250, 33), "Exportação de Gás", font=font, fill=dark_orange)
        draw.text((250, 633), date_time, font=font, fill=dark_orange)
        draw = ImageDraw.Draw(part3_resized)
        draw.text((250, 33), "Injeção de Água", font=font, fill=dark_orange)
        draw.text((250, 633), date_time, font=font, fill=dark_orange)
        
        # Informacoes adicionais start
        platforms = ["P74", "P75", "P76", "P77", "FPAB"]
        id_prefixes = {
            "part1_resized": [
                [":r17:", ":r1f:", ":r1i:", ":r1m:"],
                [":r1s:", ":r24:", ":r27:", ":r2b:"],
                [":r2h:", ":r2p:", ":r2s:", ":r30:"],
                [":r36:", ":r3e:", ":r3h:", ":r3l:"],
                [":r3r:", ":r43:", ":r46:", ":r4a:"]
            ],
            "part2_resized": [
                [":r19:", ":r1g:", ":r1j:", ":r1n:"],
                [":r1u:", ":r25:", ":r28:", ":r2c:"],
                [":r2j:", ":r2q:", ":r2t:", ":r31:"],
                [":r38:", ":r3f:", ":r3i:", ":r3m:"],
                [":r3t:", ":r44:", ":r47:", ":r4b:"]
            ],
            "part3_resized": [
                [":r1b:", ":r1h:", ":r1k:", ":r1o:"],
                [":r20:", ":r26:", ":r29:", ":r2d:"],
                [":r2l:", ":r2r:", ":r2u:", ":r32:"],
                [":r3a:", ":r3g:", ":r3j:", ":r3n:"],
                [":r3v:", ":r45:", ":r48:", ":r4c:"]
            ]
        }

        for part_name, part in [("part1_resized", part1_resized), 
                                ("part2_resized", part2_resized), 
                                ("part3_resized", part3_resized)]:
            draw = ImageDraw.Draw(part)
            regex = r"(\D+?)([\d,.]+)" if part_name == "part1_resized" else r"(\D+?)([\d,.]+)$"
            
            for i, platform in enumerate(platforms):
                if i < 3:
                    x_offset = 40 + (200 * i)
                    y_offset = 700
                else:
                    x_offset = 140 + (200 * (i - 3)) + 30  # Added more pixels for P77 and FPAB to move the text
                    y_offset = 820
                
                draw.text((x_offset, y_offset), platform, font=font, fill=dark_orange)
                
                for j, element_id in enumerate(id_prefixes[part_name][i]):
                    element = soup.find(id=element_id)
                    if element:
                        text = element.text
                        match = re.match(regex, text)
                        if match:
                            text = f"{match.group(1)} {match.group(2)}"
                    else:
                        text = "N/A"  # or any placeholder text you prefer
                    
                    draw.text((x_offset, y_offset + 20 + (j * 20)), text, font=font, fill=dark_orange)

        # Informacoes adicionais end

        # Save each part as a separate JPG
        part1_resized.save(os.path.join(save_folder, f"{filename_base}_part1.jpg"))
        part2_resized.save(os.path.join(save_folder, f"{filename_base}_part2.jpg"))
        part3_resized.save(os.path.join(save_folder, f"{filename_base}_part3.jpg"))

        # Convert each part to base64 and save to separate files
        part1_base64 = convert_to_base64(os.path.join(save_folder, f"{filename_base}_part1.jpg"))
        part2_base64 = convert_to_base64(os.path.join(save_folder, f"{filename_base}_part2.jpg"))
        part3_base64 = convert_to_base64(os.path.join(save_folder, f"{filename_base}_part3.jpg"))

        save_base64_to_file_part(save_folder, filename_base, part1_base64, "part1")
        save_base64_to_file_part(save_folder, filename_base, part2_base64, "part2")
        save_base64_to_file_part(save_folder, filename_base, part3_base64, "part3")

def split_and_save_jpg(jpg_path, save_folder, filename_base, date_time, soup):
    with Image.open(jpg_path) as img:                        
        print(f"Date and Time: {date_time}")
        width, height = img.size
        # Define a dark orange color tuple (R, G, B)
        dark_orange = (255, 165, 0)
        
        # Calculate part sizes
        part1_width = int(width * 0.415)
        part2_width = int(width * 0.273)
        part3_width = width - part1_width - part2_width

        # Calculate new height (80% of original)
        new_height = int(height * 1)

        # Split the image
        part1 = img.crop((0, 0, part1_width, new_height))
        part2 = img.crop((part1_width, 0, part1_width + part2_width, new_height))
        part3 = img.crop((part1_width + part2_width, 0, width, new_height))

        # Further slice part1 on the left side by 15%
        part1_slice_width = int(part1_width * 0.15)
        part1_sliced = part1.crop((part1_slice_width, 0, part1_width, new_height))

        # Find the largest dimensions
        max_width = max(part1_sliced.width, part2.width, part3.width)
        max_height = new_height

        # Resize all parts to the largest dimensions
        part1_resized = part1_sliced.resize((max_width, max_height), Image.LANCZOS)
        part2_resized = part2.resize((max_width, max_height), Image.LANCZOS)
        part3_resized = part3.resize((max_width, max_height), Image.LANCZOS)

        # Add watermark to part2 and part3
        font_size = 20
        try:
            # Try to use Arial font (common on Windows)
            font = ImageFont.truetype("arial.ttf", font_size)
        except IOError:
            # If Arial is not available, use default font
            font = ImageFont.load_default()

        for part in [part2_resized, part3_resized]:
            draw = ImageDraw.Draw(part)
            draw.text((10, 130), "P74", font=font, fill=dark_orange)  # Black color with 50% opacity
            draw.text((10, 237), "P75", font=font, fill=dark_orange)  # Black color with 50% opacity
            draw.text((10, 343), "P76", font=font, fill=dark_orange)  # Black color with 50% opacity
            draw.text((10, 450), "P77", font=font, fill=dark_orange)  # Black color with 50% opacity
            draw.text((10, 557), "FPAB", font=font, fill=dark_orange)  # Black color with 50% opacity

        draw = ImageDraw.Draw(part1_resized)
        draw.text((250, 57), "Produção de Óleo", font=font, fill=dark_orange)
        draw.text((250, 657), date_time, font=font, fill=dark_orange)
        draw = ImageDraw.Draw(part2_resized)
        draw.text((250, 33), "Exportação de Gás", font=font, fill=dark_orange)
        draw.text((250, 633), date_time, font=font, fill=dark_orange)
        draw = ImageDraw.Draw(part3_resized)
        draw.text((250, 33), "Injeção de Água", font=font, fill=dark_orange)
        draw.text((250, 633), date_time, font=font, fill=dark_orange)
        
        # Informacoes adicionais start
        #<------------------------------------------------------------------part1_resized
        regex = r"(\D+?)([\d,.]+)"
        draw = ImageDraw.Draw(part1_resized)
        draw.text((40, 700), "P74", font=font, fill=dark_orange)
        match = re.match(regex, soup.find(id=":r17:").text)
        draw.text((40, 720) if match else (40, 720), f"{match.group(1)} {match.group(2)}" if match else soup.find(id=":r17:").text, font=font, fill=dark_orange)
        match = re.match(regex, soup.find(id=":r1f:").text)
        draw.text((40, 740) if match else (40, 740), f"{match.group(1)} {match.group(2)}" if match else soup.find(id=":r1f:").text, font=font, fill=dark_orange)
        match = re.match(regex, soup.find(id=":r1i:").text)
        draw.text((40, 760) if match else (40, 760), f"{match.group(1)} {match.group(2)}" if match else soup.find(id=":r1i:").text, font=font, fill=dark_orange)
        match = re.match(regex, soup.find(id=":r1m:").text)
        draw.text((40, 780) if match else (40, 780), f"{match.group(1)} {match.group(2)}" if match else soup.find(id=":r1m:").text, font=font, fill=dark_orange)
        
        draw.text((240, 700), "P75", font=font, fill=dark_orange)
        match = re.match(regex, soup.find(id=":r1s:").text)
        draw.text((240, 720) if match else (240, 720), f"{match.group(1)} {match.group(2)}" if match else soup.find(id=":r1s:").text, font=font, fill=dark_orange)
        match = re.match(regex, soup.find(id=":r24:").text)
        draw.text((240, 740) if match else (240, 740), f"{match.group(1)} {match.group(2)}" if match else soup.find(id=":r24:").text, font=font, fill=dark_orange)
        match = re.match(regex, soup.find(id=":r27:").text)
        draw.text((240, 760) if match else (240, 760), f"{match.group(1)} {match.group(2)}" if match else soup.find(id=":r27:").text, font=font, fill=dark_orange)
        match = re.match(regex, soup.find(id=":r2b:").text)
        draw.text((240, 780) if match else (240, 780), f"{match.group(1)} {match.group(2)}" if match else soup.find(id=":r2b:").text, font=font, fill=dark_orange)
        
        draw.text((440, 700), "P76", font=font, fill=dark_orange)
        match = re.match(regex, soup.find(id=":r2h:").text)
        draw.text((440, 720) if match else (440, 720), f"{match.group(1)} {match.group(2)}" if match else soup.find(id=":r2h:").text, font=font, fill=dark_orange)
        match = re.match(regex, soup.find(id=":r2p:").text)
        draw.text((440, 740) if match else (440, 740), f"{match.group(1)} {match.group(2)}" if match else soup.find(id=":r2p:").text, font=font, fill=dark_orange)
        match = re.match(regex, soup.find(id=":r2s:").text)
        draw.text((440, 760) if match else (440, 760), f"{match.group(1)} {match.group(2)}" if match else soup.find(id=":r2s:").text, font=font, fill=dark_orange)
        match = re.match(regex, soup.find(id=":r30:").text)
        draw.text((440, 780) if match else (440, 780), f"{match.group(1)} {match.group(2)}" if match else soup.find(id=":r30:").text, font=font, fill=dark_orange)
        
        draw.text((140, 820), "P77", font=font, fill=dark_orange)
        match = re.match(regex, soup.find(id=":r36:").text)
        draw.text((140, 840) if match else (140, 840), f"{match.group(1)} {match.group(2)}" if match else soup.find(id=":r36:").text, font=font, fill=dark_orange)
        match = re.match(regex, soup.find(id=":r3e:").text)
        draw.text((140, 860) if match else (140, 860), f"{match.group(1)} {match.group(2)}" if match else soup.find(id=":r3e:").text, font=font, fill=dark_orange)
        match = re.match(regex, soup.find(id=":r3h:").text)
        draw.text((140, 880) if match else (140, 880), f"{match.group(1)} {match.group(2)}" if match else soup.find(id=":r3h:").text, font=font, fill=dark_orange)
        match = re.match(regex, soup.find(id=":r3l:").text)
        draw.text((140, 900) if match else (140, 900), f"{match.group(1)} {match.group(2)}" if match else soup.find(id=":r3l:").text, font=font, fill=dark_orange)
        
        draw.text((340, 820), "FPAB", font=font, fill=dark_orange)
        match = re.match(regex, soup.find(id=":r3r:").text)
        draw.text((340, 840) if match else (340, 840), f"{match.group(1)} {match.group(2)}" if match else soup.find(id=":r3r:").text, font=font, fill=dark_orange)
        match = re.match(regex, soup.find(id=":r43:").text)
        draw.text((340, 860) if match else (340, 860), f"{match.group(1)} {match.group(2)}" if match else soup.find(id=":r43:").text, font=font, fill=dark_orange)
        match = re.match(regex, soup.find(id=":r46:").text)
        draw.text((340, 880) if match else (340, 880), f"{match.group(1)} {match.group(2)}" if match else soup.find(id=":r46:").text, font=font, fill=dark_orange)
        match = re.match(regex, soup.find(id=":r4a:").text)
        draw.text((340, 900) if match else (340, 900), f"{match.group(1)} {match.group(2)}" if match else soup.find(id=":r4a:").text, font=font, fill=dark_orange)
        #<------------------------------------------------------------------part2_resized
        regex = r"(\D+?)([\d,.]+)$"
        draw = ImageDraw.Draw(part2_resized)
        draw.text((40, 700), "P74", font=font, fill=dark_orange)
        match = re.match(regex, soup.find(id=":r19:").text)
        draw.text((40, 720) if match else (40, 720), f"{match.group(1)} {match.group(2)}" if match else soup.find(id=":r19:").text, font=font, fill=dark_orange)
        match = re.match(regex, soup.find(id=":r1g:").text)
        draw.text((40, 740) if match else (40, 740), f"{match.group(1)} {match.group(2)}" if match else soup.find(id=":r1g:").text, font=font, fill=dark_orange)
        match = re.match(regex, soup.find(id=":r1j:").text)
        draw.text((40, 760) if match else (40, 760), f"{match.group(1)} {match.group(2)}" if match else soup.find(id=":r1j:").text, font=font, fill=dark_orange)
        match = re.match(regex, soup.find(id=":r1n:").text)
        draw.text((40, 780) if match else (40, 780), f"{match.group(1)} {match.group(2)}" if match else soup.find(id=":r1n:").text, font=font, fill=dark_orange)
        
        draw.text((240, 700), "P75", font=font, fill=dark_orange)
        match = re.match(regex, soup.find(id=":r1u:").text)
        draw.text((240, 720) if match else (240, 720), f"{match.group(1)} {match.group(2)}" if match else soup.find(id=":r1u:").text, font=font, fill=dark_orange)
        match = re.match(regex, soup.find(id=":r25:").text)
        draw.text((240, 740) if match else (240, 740), f"{match.group(1)} {match.group(2)}" if match else soup.find(id=":r25:").text, font=font, fill=dark_orange)
        match = re.match(regex, soup.find(id=":r28:").text)
        draw.text((240, 760) if match else (240, 760), f"{match.group(1)} {match.group(2)}" if match else soup.find(id=":r28:").text, font=font, fill=dark_orange)
        match = re.match(regex, soup.find(id=":r2c:").text)
        draw.text((240, 780) if match else (240, 780), f"{match.group(1)} {match.group(2)}" if match else soup.find(id=":r2c:").text, font=font, fill=dark_orange)
        
        draw.text((440, 700), "P76", font=font, fill=dark_orange)
        match = re.match(regex, soup.find(id=":r2j:").text)
        draw.text((440, 720) if match else (440, 720), f"{match.group(1)} {match.group(2)}" if match else soup.find(id=":r2j:").text, font=font, fill=dark_orange)
        match = re.match(regex, soup.find(id=":r2q:").text)
        draw.text((440, 740) if match else (440, 740), f"{match.group(1)} {match.group(2)}" if match else soup.find(id=":r2q:").text, font=font, fill=dark_orange)
        match = re.match(regex, soup.find(id=":r2t:").text)
        draw.text((440, 760) if match else (440, 760), f"{match.group(1)} {match.group(2)}" if match else soup.find(id=":r2t:").text, font=font, fill=dark_orange)
        match = re.match(regex, soup.find(id=":r31:").text)
        draw.text((440, 780) if match else (440, 780), f"{match.group(1)} {match.group(2)}" if match else soup.find(id=":r31:").text, font=font, fill=dark_orange)
        
        draw.text((140, 820), "P77", font=font, fill=dark_orange)
        match = re.match(regex, soup.find(id=":r38:").text)
        draw.text((140, 840) if match else (140, 840), f"{match.group(1)} {match.group(2)}" if match else soup.find(id=":r38:").text, font=font, fill=dark_orange)
        match = re.match(regex, soup.find(id=":r3f:").text)
        draw.text((140, 860) if match else (140, 860), f"{match.group(1)} {match.group(2)}" if match else soup.find(id=":r3f:").text, font=font, fill=dark_orange)
        match = re.match(regex, soup.find(id=":r3i:").text)
        draw.text((140, 880) if match else (140, 880), f"{match.group(1)} {match.group(2)}" if match else soup.find(id=":r3i:").text, font=font, fill=dark_orange)
        match = re.match(regex, soup.find(id=":r3m:").text)
        draw.text((140, 900) if match else (140, 900), f"{match.group(1)} {match.group(2)}" if match else soup.find(id=":r3m:").text, font=font, fill=dark_orange)
        
        draw.text((340, 820), "FPAB", font=font, fill=dark_orange)
        match = re.match(regex, soup.find(id=":r3t:").text)
        draw.text((340, 840) if match else (340, 840), f"{match.group(1)} {match.group(2)}" if match else soup.find(id=":r3t:").text, font=font, fill=dark_orange)
        match = re.match(regex, soup.find(id=":r44:").text)
        draw.text((340, 860) if match else (340, 860), f"{match.group(1)} {match.group(2)}" if match else soup.find(id=":r44:").text, font=font, fill=dark_orange)
        match = re.match(regex, soup.find(id=":r47:").text)
        draw.text((340, 880) if match else (340, 880), f"{match.group(1)} {match.group(2)}" if match else soup.find(id=":r47:").text, font=font, fill=dark_orange)
        match = re.match(regex, soup.find(id=":r4b:").text)
        draw.text((340, 900) if match else (340, 900), f"{match.group(1)} {match.group(2)}" if match else soup.find(id=":r4b:").text, font=font, fill=dark_orange)
        #<------------------------------------------------------------------part3_resized
        regex = r"(\D+?)([\d,.]+)$"
        draw = ImageDraw.Draw(part3_resized)
        draw.text((40, 700), "P74", font=font, fill=dark_orange)
        match = re.match(regex, soup.find(id=":r1b:").text)
        draw.text((40, 720) if match else (40, 720), f"{match.group(1)} {match.group(2)}" if match else soup.find(id=":r1b:").text, font=font, fill=dark_orange)
        match = re.match(regex, soup.find(id=":r1h:").text)
        draw.text((40, 740) if match else (40, 740), f"{match.group(1)} {match.group(2)}" if match else soup.find(id=":r1h:").text, font=font, fill=dark_orange)
        match = re.match(regex, soup.find(id=":r1k:").text)
        draw.text((40, 760) if match else (40, 760), f"{match.group(1)} {match.group(2)}" if match else soup.find(id=":r1k:").text, font=font, fill=dark_orange)
        match = re.match(regex, soup.find(id=":r1o:").text)
        draw.text((40, 780) if match else (40, 780), f"{match.group(1)} {match.group(2)}" if match else soup.find(id=":r1o:").text, font=font, fill=dark_orange)
        
        draw.text((240, 700), "P75", font=font, fill=dark_orange)
        match = re.match(regex, soup.find(id=":r20:").text)
        draw.text((240, 720) if match else (240, 720), f"{match.group(1)} {match.group(2)}" if match else soup.find(id=":r20:").text, font=font, fill=dark_orange)
        match = re.match(regex, soup.find(id=":r26:").text)
        draw.text((240, 740) if match else (240, 740), f"{match.group(1)} {match.group(2)}" if match else soup.find(id=":r26:").text, font=font, fill=dark_orange)
        match = re.match(regex, soup.find(id=":r29:").text)
        draw.text((240, 760) if match else (240, 760), f"{match.group(1)} {match.group(2)}" if match else soup.find(id=":r29:").text, font=font, fill=dark_orange)
        match = re.match(regex, soup.find(id=":r2d:").text)
        draw.text((240, 780) if match else (240, 780), f"{match.group(1)} {match.group(2)}" if match else soup.find(id=":r2d:").text, font=font, fill=dark_orange)
        
        draw.text((440, 700), "P76", font=font, fill=dark_orange)
        match = re.match(regex, soup.find(id=":r2l:").text)
        draw.text((440, 720) if match else (440, 720), f"{match.group(1)} {match.group(2)}" if match else soup.find(id=":r2l:").text, font=font, fill=dark_orange)
        match = re.match(regex, soup.find(id=":r2r:").text)
        draw.text((440, 740) if match else (440, 740), f"{match.group(1)} {match.group(2)}" if match else soup.find(id=":r2r:").text, font=font, fill=dark_orange)
        match = re.match(regex, soup.find(id=":r2u:").text)
        draw.text((440, 760) if match else (440, 760), f"{match.group(1)} {match.group(2)}" if match else soup.find(id=":r2u:").text, font=font, fill=dark_orange)
        match = re.match(regex, soup.find(id=":r32:").text)
        draw.text((440, 780) if match else (440, 780), f"{match.group(1)} {match.group(2)}" if match else soup.find(id=":r32:").text, font=font, fill=dark_orange)
        
        draw.text((140, 820), "P77", font=font, fill=dark_orange)
        match = re.match(regex, soup.find(id=":r3a:").text)
        draw.text((140, 840) if match else (140, 840), f"{match.group(1)} {match.group(2)}" if match else soup.find(id=":r3a:").text, font=font, fill=dark_orange)
        match = re.match(regex, soup.find(id=":r3g:").text)
        draw.text((140, 860) if match else (140, 860), f"{match.group(1)} {match.group(2)}" if match else soup.find(id=":r3g:").text, font=font, fill=dark_orange)
        match = re.match(regex, soup.find(id=":r3j:").text)
        draw.text((140, 880) if match else (140, 880), f"{match.group(1)} {match.group(2)}" if match else soup.find(id=":r3j:").text, font=font, fill=dark_orange)
        match = re.match(regex, soup.find(id=":r3n:").text)
        draw.text((140, 900) if match else (140, 900), f"{match.group(1)} {match.group(2)}" if match else soup.find(id=":r3n:").text, font=font, fill=dark_orange)
        
        draw.text((340, 820), "FPAB", font=font, fill=dark_orange)
        match = re.match(regex, soup.find(id=":r3v:").text)
        draw.text((340, 840) if match else (340, 840), f"{match.group(1)} {match.group(2)}" if match else soup.find(id=":r3v:").text, font=font, fill=dark_orange)
        match = re.match(regex, soup.find(id=":r45:").text)
        draw.text((340, 860) if match else (340, 860), f"{match.group(1)} {match.group(2)}" if match else soup.find(id=":r45:").text, font=font, fill=dark_orange)
        match = re.match(regex, soup.find(id=":r48:").text)
        draw.text((340, 880) if match else (340, 880), f"{match.group(1)} {match.group(2)}" if match else soup.find(id=":r48:").text, font=font, fill=dark_orange)
        match = re.match(regex, soup.find(id=":r4c:").text)
        draw.text((340, 900) if match else (340, 900), f"{match.group(1)} {match.group(2)}" if match else soup.find(id=":r4c:").text, font=font, fill=dark_orange)
        # Informacoes adicionais end

        # Save each part as a separate JPG
        part1_resized.save(os.path.join(save_folder, f"{filename_base}_part1.jpg"))
        part2_resized.save(os.path.join(save_folder, f"{filename_base}_part2.jpg"))
        part3_resized.save(os.path.join(save_folder, f"{filename_base}_part3.jpg"))

        # Convert each part to base64 and save to separate files
        part1_base64 = convert_to_base64(os.path.join(save_folder, f"{filename_base}_part1.jpg"))
        part2_base64 = convert_to_base64(os.path.join(save_folder, f"{filename_base}_part2.jpg"))
        part3_base64 = convert_to_base64(os.path.join(save_folder, f"{filename_base}_part3.jpg"))

        save_base64_to_file_part(save_folder, filename_base, part1_base64, "part1")
        save_base64_to_file_part(save_folder, filename_base, part2_base64, "part2")
        save_base64_to_file_part(save_folder, filename_base, part3_base64, "part3")

def save_base64_to_file_part(save_folder, filename_base, part_base64, part_name):
    with open(os.path.join(save_folder, f"{filename_base}_base64_{part_name}.txt"), "w") as f:
        f.write(f"{part_base64}\n")  # Write only the base64 content
    print(f"Base64 data for {part_name} saved to {filename_base}_base64_{part_name}.txt")


# Main script
if __name__ == "__main__":
    save_folder = r"C:\Users\FLSV\Downloads"  # Changed destination folder
    
    webpage_url = "
    
    screenshot_base_filename = "Videowall"  # Base filename without extension
    
    username = ""
    password = ""

    #while True:
    # Capture the screenshot and save Base64
    png_base64, jpg_base64 = capture_screenshot(webpage_url, screenshot_base_filename, username, password, save_folder)
    
    # Display first 50 characters of Base64 output
    print("PNG Base64:", png_base64[:50] + "...")  
    print("JPG Base64:", jpg_base64[:50] + "...")

    #    time.sleep(600)  # Sleep for 10 minutes (600 seconds)

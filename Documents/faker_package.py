from faker import Faker

fake = Faker(['en_IN'])  # Create Faker instance with Indian English locale


# ==================================================
# 👤 1. Personal Information
# ==================================================
print("=== PERSONAL INFORMATION ===")
print("Full Name      :", fake.name())
print("First Name     :", fake.first_name())
print("Last Name      :", fake.last_name())
print("Title          :", fake.prefix())
print("Suffix         :", fake.suffix())
print("Date of Birth  :", fake.date_of_birth())

"""
Sample Output:
=== PERSONAL INFORMATION ===
Full Name      : Rahul Sharma
First Name     : Rahul
Last Name      : Sharma
Title          : Mr.
Suffix         : Jr.
Date of Birth  : 1994-08-17
"""


# ==================================================
# 🏠 2. Address & Location
# ==================================================
print("\n=== ADDRESS INFORMATION ===")
print("Full Address   :", fake.address())
print("Street Address :", fake.street_address())
print("City           :", fake.city())
print("State          :", fake.state())
print("Country        :", fake.country())
print("Postal Code    :", fake.postcode())
print("Latitude       :", fake.latitude())
print("Longitude      :", fake.longitude())

"""
Sample Output:
=== ADDRESS INFORMATION ===
Full Address   : Flat 45, MG Road
                  Mumbai, Maharashtra 400001
Street Address : 22 Park Street
City           : Mumbai
State          : Maharashtra
Country        : India
Postal Code    : 400001
Latitude       : 19.0760
Longitude      : 72.8777
"""


# ==================================================
# 📞 3. Contact Details
# ==================================================
print("\n=== CONTACT DETAILS ===")
print("Phone Number   :", fake.phone_number())
print("Email          :", fake.email())
print("Free Email     :", fake.free_email())
print("Company Email  :", fake.company_email())
print("Website        :", fake.url())

"""
Sample Output:
=== CONTACT DETAILS ===
Phone Number   : +91 98765 43210
Email          : rahul@example.com
Free Email     : rahul123@gmail.com
Company Email  : rahul.sharma@techsolutions.in
Website        : https://www.example.in
"""


# ==================================================
# 💼 4. Job & Company
# ==================================================
print("\n=== JOB INFORMATION ===")
print("Job Title      :", fake.job())
print("Company        :", fake.company())
print("Company Suffix :", fake.company_suffix())
print("Slogan         :", fake.catch_phrase())

"""
Sample Output:
=== JOB INFORMATION ===
Job Title      : Software Engineer
Company        : Tech Solutions
Company Suffix : Pvt Ltd
Slogan         : Innovating the Future
"""


# ==================================================
# 💳 5. Financial Data
# ==================================================
print("\n=== FINANCIAL DATA ===")
print("Card Number    :", fake.credit_card_number())
print("Card Provider  :", fake.credit_card_provider())
print("IBAN           :", fake.iban())
print("SWIFT Code     :", fake.swift())
print("Currency Code  :", fake.currency_code())
print("Price Tag      :", fake.pricetag())

"""
Sample Output:
=== FINANCIAL DATA ===
Card Number    : 4539 1488 0343 6467
Card Provider  : Visa
IBAN           : IN82SBIN00000012345678
SWIFT Code     : SBININBBXXX
Currency Code  : INR
Price Tag      : ₹4,599.00
"""


# ==================================================
# 🌐 6. Internet & Tech Data
# ==================================================
print("\n=== TECH DATA ===")
print("Username       :", fake.user_name())
print("Password       :", fake.password())
print("IPv4 Address   :", fake.ipv4())
print("IPv6 Address   :", fake.ipv6())
print("MAC Address    :", fake.mac_address())
print("Domain Name    :", fake.domain_name())

"""
Sample Output:
=== TECH DATA ===
Username       : rahul94
Password       : Xy@1234!ab
IPv4 Address   : 192.168.1.12
IPv6 Address   : 2001:0db8:85a3:0000:0000:8a2e:0370:7334
MAC Address    : 00:1B:44:11:3A:B7
Domain Name    : example.in
"""


# ==================================================
# 📅 7. Date & Time
# ==================================================
print("\n=== DATE INFORMATION ===")
print("Random Date    :", fake.date())
print("Time           :", fake.time())
print("Date Time      :", fake.date_time())
print("Future Date    :", fake.future_date())
print("Past Date      :", fake.past_date())

"""
Sample Output:
=== DATE INFORMATION ===
Random Date    : 2025-06-12
Time           : 14:35:22
Date Time      : 2025-06-12 14:35:22
Future Date    : 2026-01-15
Past Date      : 2023-09-10
"""


# ==================================================
# 📄 8. Text & Content
# ==================================================
print("\n=== TEXT CONTENT ===")
print("Word           :", fake.word())
print("Sentence       :", fake.sentence())
print("Paragraph      :", fake.paragraph())
print("Text           :", fake.text())

"""
Sample Output:
=== TEXT CONTENT ===
Word           : innovation
Sentence       : Technology is transforming the world rapidly.
Paragraph      : Lorem ipsum dolor sit amet, consectetur adipiscing elit...
Text           : Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod...
"""


# ==================================================
# 🏥 9. Full Profile
# ==================================================
print("\n=== FULL PROFILE ===")
profile = fake.profile()
for key, value in profile.items():
    print(f"{key:18} : {value}")

"""
Sample Output:
=== FULL PROFILE ===
job                : Software Engineer
company            : Tech Solutions Pvt Ltd
residence          : Flat 12, MG Road
                     Mumbai 400001
current_location   : (19.0760, 72.8777)
website            : ['http://example.in']
username           : rahul123
name               : Rahul Sharma
sex                : M
address            : 22 Park Street, Delhi 110001
mail               : rahul@example.com
birthdate          : 1995-05-17
"""
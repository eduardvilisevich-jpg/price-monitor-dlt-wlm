import requests
from bs4 import BeautifulSoup
import pandas as pd
import re
import time
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from fuzzywuzzy import fuzz

# ===============================
# НАСТРОЙКИ
# ===============================

GOOGLE_SHEET_NAME = "Сравнение цен DLT vs WLM"

HEADERS = {
    "User-Agent": "Mozilla/5.0"
}

# ===============================
# GOOGLE SHEETS
# ===============================

def connect_google():
    scope = [
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/drive"
    ]
    creds = ServiceAccountCredentials.from_json_keyfile_name(
        "credentials.json", scope
    )
    client = gspread.authorize(creds)
    return client.open(GOOGLE_SHEET_NAME).sheet1

# ===============================
# DLT PARSER
# ===============================

def parse_dlt():
    print("Scanning DLT...")
    base_url = "https://dlt.by/"
    response = requests.get(base_url, headers=HEADERS)
    soup = BeautifulSoup(response.text, "html.parser")

    category_links = list(set([
        a.get("href") for a in soup.select("a[href*='dlt.by/']")
        if a.get("href") and a.get("href").startswith("https://dlt.by/")
    ]))

    products = []

    for cat in category_links:
        page = 1
        while True:
            try:
                url = f"{cat}?limit=200&page={page}"
                r = requests.get(url, headers=HEADERS, timeout=15)
                s = BeautifulSoup(r.text, "html.parser")

                items = s.select(".product-layout")
                if not items:
                    break

                for item in items:
                    try:
                        name_tag = item.select_one("h4 a")
                        if not name_tag:
                            continue

                        name = name_tag.text.strip()
                        p_url = name_tag.get("href").split("?")[0]

                        price_tag = item.select_one(".price")
                        if not price_tag:
                            continue

                        price_text = price_tag.text.replace(" ", "").replace(",", ".")
                        price_val = re.sub(r"[^\d.]", "", price_text)
                        if not price_val:
                            continue

                        price = float(price_val)

                        article_match = re.search(r"арт\.?(\d+)", name.lower())
                        article = article_match.group(1) if article_match else None

                        products.append({
                            "name": name,
                            "price": price,
                            "article": article,
                            "url": p_url
                        })
                    except:
                        continue

                next_page = s.select_one("ul.pagination li.active + li a")
                if not next_page:
                    break

                page += 1
                time.sleep(0.1)
            except:
                break

    print("DLT total raw:", len(products))

    # Remove duplicates by URL
    unique = {p["url"]: p for p in products}
    print("DLT unique:", len(unique))

    return list(unique.values())

# ===============================
# WLM PARSER
# ===============================

def parse_wlm():
    print("Scanning WLM...")
    categories = [
        "https://new.wlm.by/plitkorezy-elektricheskie",
        "https://new.wlm.by/plitkorezy-ruchnye",
        "https://new.wlm.by/instrument-dlya-ukladki-plitki",
        "https://new.wlm.by/almaznyj-instrument"
    ]

    products = []

    for cat in categories:
        try:
            r = requests.get(cat, headers=HEADERS, timeout=15)
            s = BeautifulSoup(r.text, "html.parser")

            items = s.select(".t-store__card")

            for item in items:
                try:
                    name_tag = item.select_one(".t-store__card-title")
                    if not name_tag:
                        continue

                    name = name_tag.text.strip()

                    price_tag = item.select_one(".js-store-prod-price-val")
                    if not price_tag:
                        continue

                    price_val = price_tag.get("data-product-price")
                    price = float(price_val) if price_val else 0

                    article_match = re.search(r"арт\.?(\d+)", name.lower())
                    article = article_match.group(1) if article_match else None

                    products.append({
                        "name": name,
                        "price": price,
                        "article": article,
                        "url": cat
                    })
                except:
                    continue
        except:
            continue

    print("WLM total:", len(products))

    return products

# ===============================
# COMPARISON
# ===============================

def compare(dlt, wlm):
    print("Comparing...")
    rows = [["Артикул", "Название DLT", "Название WLM",
             "Цена DLT", "Цена WLM", "Разница", "Разница %"]]

    for d in dlt:
        match = None

        # match by article
        if d["article"]:
            match = next((w for w in wlm if w["article"] == d["article"]), None)

        # match by name
        if not match:
            for w in wlm:
                score = fuzz.token_sort_ratio(d["name"].lower(), w["name"].lower())
                if score > 85:
                    match = w
                    break

        if match:
            diff = round(match["price"] - d["price"], 2)
            perc = round((diff / d["price"]) * 100, 2) if d["price"] else 0

            rows.append([
                d["article"] or "",
                d["name"],
                match["name"],
                d["price"],
                match["price"],
                diff,
                f"{perc}%"
            ])

    print("Matches found:", len(rows)-1)
    return rows

# ===============================
# MAIN
# ===============================

if __name__ == "__main__":
    sheet = connect_google()

    dlt_products = parse_dlt()
    wlm_products = parse_wlm()

    result = compare(dlt_products, wlm_products)

    sheet.clear()
    sheet.update("A1", result)

    print("✅ DONE")

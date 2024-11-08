import os
from datetime import datetime

import httpx
import asyncio
import pandas as pd
import copy
import time

from dotenv import load_dotenv
from httpcore import TimeoutException
from httpx import AsyncClient

load_dotenv()

URL_PRODUCT = "https://app.retailed.io/api/v1/scraper/goat/product"
URL_PRICES = "https://app.retailed.io/api/v1/scraper/goat/prices"
API_KEY = os.getenv("API_KEY")
PHOTO_INDEX = [0, 3, 5, 7, 8, 9]

async_client = httpx.AsyncClient()

semaphore = asyncio.Semaphore(2)

async def get_product_from_goat(client: AsyncClient, url: str, headers, query, retries=3, delay=2):
    for attempt in range(retries):
        try:
            response = await client.get(url, headers=headers, params=query, timeout=40)
            if response.status_code == 200:
                return response.json()
            else:
                print(f"Error: Received status code {response.status_code}")
                print(f"Response content: {response.text}")
                return None
        except TimeoutException:
            print(f"Timeout error. Attempt {attempt + 1}/{retries}")

        time.sleep(delay)
    print("Max retries reached, returning None")
    return None


async def get_photo(photo_list: list, photo_index: list) -> str:
    if photo_list:
        res = [photo_list[2]["mainPictureUrl"]] if len(photo_list) else [photo_list[0]["mainPictureUrl"]]
        for i in range(len(photo_list)):
            if i in photo_index:
                res.append(photo_list[i]["mainPictureUrl"])

        return "; ".join(res)


async def parse_api(product_url, prices_url, headers, product_query):
    async with semaphore:
        async with AsyncClient() as client:
            product_json = await get_product_from_goat(
                client=client,
                url=product_url,
                headers=headers,
                query=product_query
            )

            if not product_json:
                return

            product_id = product_json.get("id")
            prices_query = {"query": str(product_id)}

            prices_json = await get_product_from_goat(
                client=client,
                url=prices_url,
                headers=headers,
                query=prices_query
            )

            res_list = []
            if prices_json:
                base_res_dict = {
                    "id": product_json.get("id"),
                    "Родительський артикул": product_json.get("sku"),
                    "Артикул для отображения на сайте": product_json.get("sku"),
                    "Название(RU)": product_json.get("name"),
                    "Название(UA)": product_json.get("name"),
                    "Название модификации(RU)": product_json.get("name"),
                    "Название модификации(UA)": product_json.get("name"),
                    "Матеріал верха": product_json.get("upperMaterial"),
                    "Назва бренду": product_json.get("brandName"),
                    "Розділ": product_json.get("brandName"),
                    "Категорія товару": ", ".join(product_json.get("category")),
                    "Для кого": product_json.get("singleGender"),
                    "Валюта": product_json.get("localizedSpecialDisplayPriceCents")["currency"],
                    "Фото": await get_photo(product_json.get("productTemplateExternalPictures"), PHOTO_INDEX),
                    "Описание товара(RU)": product_json.get("story"),
                    "Описание товара(UA)": product_json.get("story"),
                    "Короткое описание(RU)": product_json.get("details"),
                    "Короткое описание(UA)": product_json.get("details"),
                    "Колекція": product_json.get("silhouette"),
                    "Тип": product_json.get("productType"),
                    "Колір": product_json.get("color"),
                    "Дата релізу": (
                        datetime.fromisoformat(product_json.get("releaseDate")[:-1]).strftime("%d.%m.%Y")
                        if product_json.get("releaseDate") else "Намає інформації"
                    )
                }

                total_count_squ = 0
                size_set = set()
                for value in prices_json:
                    size = value["sizeOption"]["presentation"]
                    if size in size_set:
                        continue
                    size_set.add(size)

                    res_dict = copy.deepcopy(base_res_dict)
                    res_dict["Артикул"] = (
                        product_json.get("sku") if total_count_squ == 0
                        else f"{product_json.get('sku')}-{total_count_squ}"
                    )
                    res_dict["Розмір"] = size
                    res_dict["Ціна"] = (
                        round(value["lastSoldPriceCents"]["amount"] / 100, 2)  if value["lastSoldPriceCents"]["amount"] != 0
                        else round(value["lastSoldPriceCents"]["amount"] / 100, 2)
                    )
                    res_dict["Стара ціна"] = (
                        round(value["lowestPriceCents"]["amount"] / 100, 2) if "amount" in value["lowestPriceCents"].keys()
                        else round(value["lastSoldPriceCents"]["amount"] / 100, 2)
                    )
                    res_dict["Наявність"] = (
                        "В наявності" if value["lastSoldPriceCents"]["amount"] != 0 else "Немає в наявності"
                    )
                    res_dict["Отображать"] = "Да" if value["lastSoldPriceCents"]["amount"] != 0 else "Нет"

                    total_count_squ += 1
                    res_list.append(res_dict)

            return res_list


async def main():
    products_df = pd.read_json("C:\\Users\\Bohdan Kravets\\Desktop\\Bohdan Work\\GOAT\\GOAT API\\parse_goat\\sneackers.json")
    product_urls = products_df['sneaker_link'].tolist()

    headers = {"x-api-key": API_KEY}

    all_results = []
    tasks = []

    for url in product_urls:
        query = {"query": url}
        task = parse_api(URL_PRODUCT, URL_PRICES, headers, query)
        tasks.append(task)

    results = await asyncio.gather(*tasks)

    for res in results:
        if res:
            all_results.extend(res)

    df = pd.DataFrame(all_results)
    df.to_excel("sneakers.xlsx", index=False)


asyncio.run(main())

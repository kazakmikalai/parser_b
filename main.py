import asyncio
from db.supabase_client import get_supabase
from parsers.kwork.parser import KworkParser
from utils.logger import logger

async def parse_all_categories():
    supabase = get_supabase()
    res = supabase.table("categories").select("*").execute()
    categories = res.data
    logger.info(f"Получено {len(categories)} категорий из базы данных.")

    parent_category_ids = {category["parent_category_id"] for category in categories if category["parent_category_id"] is not None}
    logger.info(f"Найдено {len(parent_category_ids)} родительских категорий.")
    leaf_categories = [category for category in categories if category["id"] not in parent_category_ids]
    logger.info(f"Будет парситься {len(leaf_categories)} конечных категорий.")

    total_new_projects = 0  # Общий счетчик новых проектов

    for category in leaf_categories:
        logger.info(f"Начинаем парсинг категории: {category['name']} (ID: {category['id']})")
        parser = KworkParser(category)
        await parser.parse()
        total_new_projects += parser.new_projects_counter  # Добавляем к общему счетчику

    logger.info(f"=== ПАРСИНГ ЗАВЕРШЕН ===")
    logger.info(f"Всего новых проектов найдено: {total_new_projects}")
    logger.info(f"Обработано категорий: {len(leaf_categories)}")

async def main():
    await parse_all_categories()

if __name__ == "__main__":
    asyncio.run(main())

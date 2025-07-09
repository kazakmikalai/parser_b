from playwright.async_api import async_playwright
from db.supabase_client import get_supabase
from utils.logger import logger
import asyncio
from uuid import uuid4
from datetime import datetime
import asyncio
import random
import logging
import re

class KworkParser:
    def __init__(self, category):
        self.category = category
        self.supabase = get_supabase()
        self.base_url = category['url']
        self.user_data_dir = "./.playwright"
        self.new_projects_counter = 0  # Добавляем счетчик новых проектов
         # ✅ Добавляем логгер
        self.logger = logging.getLogger(f"{__name__}.{self.category['name']}")

    async def parse(self):
        """Парсинг проектов из категории."""
        if not self.base_url:
            logger.warning(f"URL для категории {self.category['name']} не найден")
            return

        # Проверяем, что URL относится к бирже kwork.ru
        if not self.base_url.startswith("https://kwork.ru/projects"):
            logger.info(f"Пропускаем категорию {self.category['name']}: URL не относится к бирже kwork.ru")
            return

        await self._init_browser()

        try:
            page_num = 1
            while True:
                # Формируем URL для текущей страницы
                if page_num == 1:
                    url = self.base_url  # Используем URL из базы для первой страницы
                else:
                    url = f"{self.base_url}&page={page_num}"  # Добавляем параметр page для последующих страниц

                if not await self._load_page(url):
                    break

                # Добавляем случайную задержку между запросами (от 3 до 7 секунд)
                await asyncio.sleep(random.uniform(3, 7))

                # Логируем содержимое страницы для отладки
                try:
                    page_content = await self.page.content()
                    logger.debug(f"Содержимое страницы {url}:\n{page_content[:1000]}...")  # Логируем первые 1000 символов
                except Exception as e:
                    logger.error(f"Ошибка при получении содержимого страницы: {str(e)}")

                # Проверяем различные селекторы для определения загрузки контента
                selectors = [
                    ".wants-content",
                    ".project-list",
                    ".want-card",
                    ".f15"  # Селектор для сообщения об отсутствии проектов
                ]

                found_selector = None
                for selector in selectors:
                    try:
                        element = await self.page.wait_for_selector(selector, timeout=10000)  # Уменьшаем таймаут для каждого селектора
                        if element:
                            found_selector = selector
                            logger.info(f"Найден селектор: {selector}")
                            break
                    except Exception as e:
                        logger.debug(f"Селектор {selector} не найден: {str(e)}")
                        continue

                if not found_selector:
                    logger.error(f"Не удалось найти ни один из ожидаемых селекторов на странице {url}")
                    break

                # Если нашли селектор с сообщением об отсутствии проектов
                if found_selector == ".f15":
                    no_projects_text = await self.page.query_selector(".f15")
                    if no_projects_text:
                        text = await no_projects_text.inner_text()
                        if "Нет ни одного доступного проекта" in text:
                            logger.info(f"На странице {url} нет проектов, завершаем парсинг категории: {self.category['name']}")
                            break

                # Проверяем наличие карточек проектов
                cards = await self.page.query_selector_all(".want-card")
                logger.info(f"Найдено карточек проектов на странице: {len(cards)}")
                
                # Если карточек нет, попробуем подождать еще немного
                if not cards:
                    logger.info("Карточки не найдены, ждем дополнительно...")
                    await asyncio.sleep(3)
                    cards = await self.page.query_selector_all(".want-card")
                    logger.info(f"После ожидания найдено карточек: {len(cards)}")
                
                if not cards:
                    logger.info(f"На странице {url} нет карточек проектов, завершаем парсинг категории: {self.category['name']}")
                    break

                # Проверим содержимое первой карточки для диагностики
                if cards:
                    first_card = cards[0]
                    try:
                        card_html = await first_card.inner_html()
                        logger.debug(f"HTML первой карточки (первые 500 символов): {card_html[:500]}...")
                        
                        # Проверим наличие заголовка в первой карточке
                        title_el = await first_card.query_selector("h1 a")
                        if title_el:
                            title_text = await title_el.inner_text()
                            title_href = await title_el.get_attribute("href")
                            logger.debug(f"Заголовок первой карточки: {title_text}")
                            logger.debug(f"Href первой карточки: {title_href}")
                        else:
                            logger.warning("Заголовок не найден в первой карточке!")
                            
                            # Попробуем найти заголовок другими способами
                            alternative_selectors = ["h1", "h2", "h3", ".title", ".project-title", "a[href*='/projects/']"]
                            for selector in alternative_selectors:
                                alt_el = await first_card.query_selector(selector)
                                if alt_el:
                                    alt_text = await alt_el.inner_text()
                                    logger.debug(f"Найден альтернативный заголовок с селектором '{selector}': {alt_text}")
                            
                    except Exception as e:
                        logger.error(f"Ошибка при анализе первой карточки: {e}")
                else:
                    # Если карточек нет, проверим, что есть на странице
                    logger.warning("Карточки не найдены, проверяем структуру страницы...")
                    try:
                        page_content = await self.page.content()
                        logger.debug(f"Содержимое страницы (первые 1000 символов): {page_content[:1000]}...")
                        
                        # Проверим наличие различных селекторов
                        test_selectors = [".want-card", ".project-card", ".card", ".item", ".project"]
                        for selector in test_selectors:
                            elements = await self.page.query_selector_all(selector)
                            logger.info(f"Селектор '{selector}': найдено {len(elements)} элементов")
                            
                    except Exception as e:
                        logger.error(f"Ошибка при анализе страницы: {e}")

                logger.info(f"На странице {page_num} найдено {len(cards)} карточек.")

                # Обрабатываем каждую карточку
                existing_projects_in_row = 0  # Счетчик существующих проектов подряд
                processed_cards = 0  # Счетчик обработанных карточек
                
                for i, card in enumerate(cards):
                    logger.debug(f"Обрабатываем карточку {i+1}/{len(cards)}")
                    project = await self.extract_project(card)
                    
                    if project:
                        logger.debug(f"Проект извлечен: {project['title']}")
                        save_result = await self.save_project(project)
                        logger.info(f"Результат save_project для '{project['title']}': {save_result}")
                        
                        if save_result: # Проверяем, существует ли проект
                            existing_projects_in_row += 1
                            logger.info(f"Проект уже существует: {project['title']}")
                            
                            # Если встретили 3 существующих проекта подряд, прекращаем парсинг
                            if existing_projects_in_row >= 3:
                                logger.info(f"Встречено {existing_projects_in_row} существующих проектов подряд, прекращаем парсинг категории: {self.category['name']}")
                                break
                        else:
                            existing_projects_in_row = 0  # Сбрасываем счетчик при новом проекте
                            self.new_projects_counter += 1  # Увеличиваем счетчик для новых проектов
                            logger.info(f"НОВЫЙ ПРОЕКТ НАЙДЕН: {project['title']}")
                    else:
                        logger.warning(f"Проект не был извлечен из карточки {i+1} на странице {page_num}")
                    
                    processed_cards += 1
                
                logger.info(f"Обработано карточек: {processed_cards}, новых проектов: {self.new_projects_counter}")

                # Если встретили много существующих проектов, прекращаем парсинг
                if existing_projects_in_row >= 3:
                    break

                page_num += 1
                if page_num > 1:  # Если это не первая страница
                    break

        finally:
            # Закрываем браузер и контекст
            if hasattr(self, 'context'):
                await self.context.close()
            if hasattr(self, 'browser'):
                await self.browser.stop()
            logger.info(f"Браузер закрыт. Всего новых проектов в категории {self.category['name']}: {self.new_projects_counter}")

    async def _init_browser(self):
        """Инициализация браузера с настройками."""
        try:
            self.browser = await async_playwright().start()
            self.context = await self.browser.chromium.launch_persistent_context(
                user_data_dir=self.user_data_dir,
                headless=True,
                args=[
                    '--disable-web-security',  # Отключаем CORS
                    '--disable-features=IsolateOrigins,site-per-process',  # Отключаем изоляцию сайтов
                    '--disable-site-isolation-trials',
                    '--disable-features=BlockInsecurePrivateNetworkRequests',
                    '--disable-features=CrossOriginOpenerPolicy',
                    '--disable-features=CrossOriginEmbedderPolicy',
                    '--disable-features=CrossOriginResourcePolicy',
                    '--no-sandbox',
                    '--disable-setuid-sandbox',
                    '--disable-dev-shm-usage',
                    '--disable-accelerated-2d-canvas',
                    '--disable-gpu',
                    '--window-size=1920,1080',
                ],
                viewport={'width': 1920, 'height': 1080},
                user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36',
                ignore_https_errors=True,
                bypass_csp=True,  # Обходим Content Security Policy
            )
            self.page = await self.context.new_page()
            
            # Устанавливаем таймауты
            self.page.set_default_timeout(30000)  # 30 секунд для всех операций
            self.page.set_default_navigation_timeout(30000)  # 30 секунд для навигации
            
            # Включаем перехват запросов
            await self.page.route("**/*", lambda route: route.continue_())
            
            # Устанавливаем обработчик ошибок
            self.page.on("pageerror", lambda err: logger.error(f"Ошибка на странице: {err}"))
            self.page.on("console", lambda msg: logger.debug(f"Консоль: {msg.text}"))
            
            logger.info("Браузер запущен.")
        except Exception as e:
            logger.error(f"Ошибка при инициализации браузера: {str(e)}")
            raise

    async def _load_page(self, url: str) -> bool:
        """Загрузка страницы с повторными попытками."""
        max_retries = 3
        retry_delay = 5
        
        for attempt in range(max_retries):
            try:
                logger.info(f"Переход на страницу: {url}")
                response = await self.page.goto(url, wait_until="networkidle")
                
                if not response:
                    logger.error(f"Не удалось загрузить страницу {url}")
                    return False
                
                if response.status != 200:
                    logger.error(f"Ошибка HTTP {response.status} при загрузке {url}")
                    return False
                
                # Ждем загрузки основного контента
                try:
                    await self.page.wait_for_selector('.wants-content', timeout=10000)
                except Exception as e:
                    logger.warning(f"Не удалось дождаться загрузки .wants-content: {str(e)}")
                
                # Ждем загрузки JavaScript
                try:
                    await self.page.wait_for_function("""
                        () => {
                            return document.readyState === 'complete' && 
                                   !document.querySelector('.loading') &&
                                   window.performance.timing.loadEventEnd > 0;
                        }
                    """, timeout=10000)
                    logger.info("JavaScript загружен")
                except Exception as e:
                    logger.warning(f"Не удалось дождаться полной загрузки JavaScript: {str(e)}")
                
                # Проверяем наличие ошибок в консоли
                console_errors = await self.page.evaluate("""
                    () => {
                        const errors = [];
                        const originalConsoleError = console.error;
                        console.error = (...args) => {
                            errors.push(args.join(' '));
                            originalConsoleError.apply(console, args);
                        };
                        return errors;
                    }
                """)
                
                if console_errors:
                    logger.error(f"Ошибки в консоли браузера: {console_errors}")
                
                logger.info(f"Страница {url} успешно загружена")
                return True
                
            except Exception as e:
                logger.error(f"Попытка {attempt + 1}/{max_retries} не удалась: {str(e)}")
                if attempt < max_retries - 1:
                    await asyncio.sleep(retry_delay)
                    continue
                return False
        
        return False

    async def extract_project(self, card):
        try:
            title_el = await card.query_selector("h1 a")
            title = await title_el.inner_text()
            href = await title_el.get_attribute("href")
            url = "https://kwork.ru" + href
            
            logger.info(f"=== ИЗВЛЕЧЕНИЕ ПРОЕКТА ===")
            logger.info(f"Заголовок: {title}")
            logger.info(f"Извлеченный href: '{href}'")
            logger.info(f"Сформированный URL: '{url}'")
            logger.info(f"Длина href: {len(href) if href else 0}")
            logger.info(f"Длина URL: {len(url)}")

            desc_el = await card.query_selector(".wants-card__description-text>.breakwords.first-letter>.d-inline")
            description = await desc_el.inner_text() if desc_el else ""
            logger.debug(f"Извлечено описание: {description[:50]}...")
            

            desired_budget_el = await card.query_selector(".wants-card__header-right-block")
            desired_budget = await desired_budget_el.inner_text() if desired_budget_el else ""
            # Удаляем слово "бюджет"
            desired_budget = re.sub(r"(?i)\sбюджет\b", "", desired_budget).strip()
            logger.debug(f"Извлечен желаемый бюджет: {desired_budget}")

            acceptable_budget_el = await card.query_selector(".wants-card__description-higher-price")
            acceptable_budget = await acceptable_budget_el.inner_text() if acceptable_budget_el else ""
            logger.debug(f"Извлечен приемлемый бюджет: {acceptable_budget}")

            proposals_count = await self.extract_proposals(card)
            logger.debug(f"Извлечено количество предложений: {proposals_count}")
            rating = await self.extract_rating(card)
            logger.debug(f"Извлечен рейтинг клиента: {rating}")

            project_data = {
                "id": str(uuid4()),
                "market_id": self.category["market_id"],
                "title": title,
                "description": description.strip(),
                "url": url,
                "desired_budget": desired_budget,
                "acceptable_budget": acceptable_budget,
                "proposals_count": proposals_count,
                "client_rating": rating,
                "category_id": self.category["id"],
                "cat_project": self.category["name"],
            }
            
            logger.info(f"Сформирован проект: {title} - {url}")
            return project_data
            
        except Exception as e:
            logger.error(f"Ошибка при извлечении проекта: {e}", exc_info=True)
            return None

    async def extract_budget(self, card, label):
        budget = await card.inner_text()
        if label in budget:
            try:
                return budget.split(label + ": до")[1].split("₽")[0].strip()
            except:
                return ""
        return ""

    async def extract_proposals(self, card):
        try:
            text = await card.inner_text()
            for line in text.splitlines():
                if "Предложений:" in line:
                    return int(line.split("Предложений:")[-1].strip())
        except:
            pass
        return None

    async def extract_rating(self, card):
        try:
            txt = await card.inner_text()
            if "Нанято:" in txt:
                return txt.split("Нанято:")[-1].split("%")[0].strip() + "%"
        except:
            pass
        return None

    async def save_project(self, project):
        """Асинхронное сохранение проекта в базу данных."""
        try:
            project_url = project["url"]
            project_id = project.get("id") or str(uuid4())
            project["id"] = project_id
            project["created_at"] = datetime.utcnow().isoformat()
            project["parsed_at"] = datetime.utcnow().isoformat()

            loop = asyncio.get_event_loop()

            # Проверка по URL
            response = await loop.run_in_executor(
                None, lambda: self.supabase.table("projects").select("id").eq("url", project_url).execute()
            )

            if response.data:
                self.logger.info(f"Проект уже существует: {project_url}")
                return True

            # Вставка проекта
            await loop.run_in_executor(
                None, lambda: self.supabase.table("projects").insert(project).execute()
            )

            # Получаем дерево категорий
            category_ids = await self.get_category_tree(project["category_id"])

            # Привязка к категориям
            for cat_id in category_ids:
                await loop.run_in_executor(
                    None, lambda: self.supabase.table("project_categories").insert({
                        "project_id": project_id,
                        "category_id": cat_id
                    }).execute()
                )

            self.logger.info(f"Проект сохранён: {project_url}")
            return False

        except Exception as e:
            self.logger.error(f"Ошибка при сохранении проекта: {e}")
            return False

    async def get_category_tree(self, category_id):
        """Асинхронно получает дерево категорий (включая родителей)."""
        try:
            loop = asyncio.get_event_loop()
            categories = [category_id]

            while True:
                result = await loop.run_in_executor(
                    None, lambda: self.supabase.table("categories").select("parent_category_id").eq("id", category_id).execute()
                )
                data = result.data
                if not data or not data[0].get("parent_category_id"):
                    break

                category_id = data[0]["parent_category_id"]
                categories.append(category_id)

            return categories

        except Exception as e:
            self.logger.error(f"Ошибка при получении дерева категорий: {e}")
            return [category_id]

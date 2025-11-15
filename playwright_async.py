import os
import random
import asyncio
import calendar
import typing as t
from uuid import uuid4
from pathlib import Path
from datetime import datetime, timedelta

from playwright.async_api import Playwright, Browser, BrowserContext, Page, ElementHandle, TimeoutError, Locator, FrameLocator

try:
    from loggers.logger import logger
    from utils.decorators import utils
    from settings.config import config
    from utils.authenticator import generate_otp
    from utils.exceptions import BrowserExceptions
    from utils.captcha_solver import solve_captcha
    from database.database import db
except ImportError as ie:
    exit(f"{ie} :: {Path(__file__).resolve()}")


class PlaywrightAsync:
    def __init__(self, user_id: str, port: int):
        self.user_id: str = user_id
        self.port: int = port
        self.page: t.Optional[Page] = None
        self.browser: t.Optional[Browser] = None
        self.context: t.Optional[BrowserContext] = None
        self.endpoint_url: str = f"http://127.0.0.1:{self.port}"
        self.base_url: str = "https://sellercentral.amazon.com/"

    @utils.async_exception
    async def click(self, element: ElementHandle, hover: bool = True, focus: bool = True, offset: bool = True) -> None:
        if not await element.is_visible():
            logger.warning(f"element is not visible :: {element}")
            return

        # try:
        #     await element.scroll_into_view_if_needed(timeout=5000)
        #     logger.info("scrolled to element")
        # except Exception as e:
        #     logger.error(f"scroll error :: {e}")

        if hover:
            try:
                await element.hover(timeout=5000)
                logger.info("hover completed")
                await asyncio.sleep(random.uniform(0.2, 0.8))
            except Exception as e:
                logger.error(f"hover error :: {e}")

        if focus:
            try:
                await element.focus()
                logger.info("focus completed")
                await asyncio.sleep(random.uniform(0.1, 0.5))
            except Exception as e:
                logger.error(f"focus error :: {e}")

        try:
            bounding_box: dict = await element.bounding_box()
            if bounding_box and offset:
                x_offset: float = random.uniform(-min(20, bounding_box["width"] / 2), min(20, bounding_box["width"] / 2))
                y_offset: float = random.uniform(-min(20, bounding_box["height"] / 2), min(20, bounding_box["height"] / 2))
                position: dict = {"x": x_offset, "y": y_offset}
            else:
                position = None

            await element.click(position=position, timeout=10000, force=False)
            logger.info("element was clicked")
        except TimeoutError:
            try:
                await element.evaluate("el => el.click()")
                logger.info("JS click executed")
            except Exception as e:
                logger.error(f"JS clicking error :: {e}")
        except Exception as e:
            logger.error(f"element clicking error :: {e}")
        # try:
        #     await element.click(
        #         position={"x": random.randint(-20, 20), "y": random.randint(-20, 20)} if offset else None,
        #         timeout=5000
        #     )
        #     logger.info("element was clicked")
        # except Exception as e:
        #     logger.error(f"element clicking error :: {e}")

    @utils.async_exception
    async def scroll_to_element(self, element: ElementHandle) -> None:
        bounding_box: dict = await element.bounding_box()
        if not bounding_box:
            logger.error("bounding box not found for element")
            return

        # scroll_y: int = await self.page.evaluate("window.scrollY")
        viewport_height: int = await self.page.evaluate("window.innerHeight")

        if 0 <= bounding_box["y"] <= viewport_height - bounding_box["height"]:
            logger.info("element is already in viewport")
            return

        # direction: bool = bounding_box["y"] > scroll_y + viewport_height or bounding_box["y"] < scroll_y
        # viewport_height: int = await self.page.evaluate("window.innerHeight")
        direction: bool = bounding_box["y"] > viewport_height / 2

        for _ in range(30):
            logger.info(f"scrolling direction :: {'DOWN' if direction else 'UP'}")
            for _ in range(random.randint(5, 10)):
                step = random.randint(30, 100)
                await self.page.mouse.wheel(0, step if direction else -step)
                await asyncio.sleep(random.uniform(0.2, 0.4))

            new_bounding_box: dict = await element.bounding_box()
            if new_bounding_box and 0 <= new_bounding_box["y"] <= viewport_height - new_bounding_box["height"]:
                logger.info("element is in viewport")
                return

        raise BrowserExceptions.ElementNotFoundError(element=element)

    @utils.async_exception
    async def get_date(self, period: str, month_name: bool = False) -> tuple:
        month = year = None
        now: datetime = datetime.now()

        if period == "current_month":
            month: int = now.month
            year: int = now.year
        elif period == "previous_month":
            if now.month > 1:
                year: int = now.year
                month: int = now.month - 1
            else:
                year: int = now.year - 1
                month: int = 12

        return calendar.month_name[month] if month_name else month, year

    @utils.async_exception
    async def set_date(
            self,
            period: str,
            element: t.Optional[ElementHandle] = None,
            service_name: t.Optional[str] = None,
            category: t.Optional[str] = None
    ) -> str | None:
        start_date = end_date = None

        month, year = await self.get_date(period="previous_month")

        if period == "current_month":
            start_date: str = f"{datetime.now().month}/01/{datetime.now().year}"
            delta: timedelta = timedelta(days=0) if datetime.now().day == 1 else timedelta(days=1)
            end_date: str = (datetime.now() - delta).strftime("%m/%d/%Y")
        elif period == "previous_month":
            last_day: str = calendar.monthrange(year, month)[1]
            month: str = f"0{month}" if month < 10 else str(month)
            start_date: str = f"{month}/01/{year}"
            end_date: str = f"{month}/{last_day}/{year}"
        elif period == "full_year":
            last_day: str = calendar.monthrange(year, month)[1]
            start_date: str = f"01/01/{year}"
            end_date: str = f"{month}/{last_day}/{year}"
        elif period == "last_week":
            now: datetime = datetime.now()
            start_date: str = (now - timedelta(days=8)).strftime("%m/%d/%Y")
            end_date: str = (now - timedelta(days=2)).strftime("%m/%d/%Y")
        else:
            now: datetime = datetime.now()
            days: int = 1 if period.startswith("1") else (3 if period.startswith("3") else None)
            start_date = end_date = (now - timedelta(days=days)).strftime("%m/%d/%Y")

        if service_name or category:
            if service_name == "shipments" or category == "shipment_awd_inbound":
                await self.run_js("set_date_shipments.js", element=element, start_date=start_date, end_date=end_date)
            elif service_name in ["business_reports", "payments"]:
                await self.run_js("set_date_start.js", start_date)
                await asyncio.sleep(10)
                await self.run_js("set_date_end.js", end_date)
                return f"{start_date}-{end_date}"
            elif service_name == "awd":
                month, year = await self.get_date(period=period, month_name=True)
                await self.run_js("set_awd_year.js", year)
                await asyncio.sleep(10)
                await self.run_js("set_awd_month.js", month)
        else:
            expression: str = """
                (element, {startDate, endDate}) => {
                element.setAttribute('start-value', startDate);
                element.setAttribute('end-value', endDate);
                }
                """
            await element.evaluate(expression=expression, arg={"startDate": start_date, "endDate": end_date})

    @utils.async_exception
    async def save_screenshot(self, selector: str) -> None:
        file_name: str = f"{datetime.now().date().isoformat()}_{selector}_{uuid4()}.png"
        file_path: str = os.path.join(config.screenshots_path, file_name)

        if self.page:
            await self.page.screenshot(path=file_path, full_page=True)

    @utils.async_exception
    async def wait_for_selector(self, selector: str, timeout: int = 15000, save_screen: bool = False) -> ElementHandle:
        try:
            element: ElementHandle = await self.page.wait_for_selector(selector=selector, timeout=timeout)
            logger.info(f"selector found :: {selector}")
            return element
        except TimeoutError:
            if save_screen:
                await self.save_screenshot(selector=selector)
            logger.error(f"selector not found :: {selector}")

    async def _try_connect_and_navigate(self, playwright: Playwright, use_existing_context: bool = True) -> bool:
        self.browser: Browser = await playwright.chromium.connect_over_cdp(endpoint_url=self.endpoint_url)

        if use_existing_context:
            self.context: BrowserContext = self.browser.contexts[0]
            self.page: Page = self.context.pages[0]
        else:
            self.context: BrowserContext = await self.browser.new_context()
            self.page: Page = await self.context.new_page()

        await self.page.goto(url=self.base_url, timeout=60000)

    @utils.async_exception
    async def connect_cdp_session(self, playwright: Playwright) -> bool:
        try:
            await self._try_connect_and_navigate(playwright, use_existing_context=True)
        except Exception as e:
            if "detached" in str(e).lower():
                logger.warning("Frame detached, creating new context")
                try:
                    await self._try_connect_and_navigate(playwright, use_existing_context=False)
                except Exception as ex:
                    logger.error(f"failed to create new context: {ex}")
                    return False
            else:
                logger.error(f"connection error :: {self.endpoint_url}: {e}")
                return False

        logger.info(f"connection established to existed browser session :: {self.endpoint_url}")
        return True

    @utils.async_exception
    async def run_js(
            self,
            js_file: str,
            *args,
            element: t.Optional[ElementHandle] = None,
            start_date: t.Optional[str] = None,
            end_date: t.Optional[str] = None
    ) -> t.Any:
        js_file_path: str = os.path.join(config.js_path, js_file)
        js_code: str = Path(js_file_path).read_text(encoding="utf-8")

        if element and start_date and end_date:
            return await element.evaluate(js_code, arg={"startDate": start_date, "endDate": end_date})

        return await self.page.evaluate(js_code, args[0] if args else None)

    @utils.async_exception
    async def is_logged(self, reload: bool = False) -> bool:
        if reload:
            await self.page.reload(timeout=60000, wait_until="load")
        else:
            await self.page.goto(url=config.URL["base_url"], timeout=60000)

        await asyncio.sleep(5)

        if await self.wait_for_selector(selector="//div[@aria-label='Settings']", save_screen=False):
            logger.info("already logged in")
            return True

        logger.warning("not logged in")
        return False

    @utils.async_exception
    async def login(self) -> bool:
        is_opened: bool = False
        for _ in range(3):
            try:
                await self.page.goto(url=self.base_url, timeout=60000)
                is_opened: bool = True
                logger.info(f"page is opened :: {self.base_url}")
                break
            except (BrowserExceptions.PageError, TimeoutError):
                logger.warning(f"page is not opened :: {self.base_url}")
                continue

        if not is_opened:
            raise BrowserExceptions.PageError()

        await asyncio.sleep(5)

        # login_button: ElementHandle = await self.wait_for_selector(selector="//strong[text()='Log in']")
        # if not login_button:
        #     logger.error("not found login button")
        #     return
        #
        # await login_button.click()
        # await asyncio.sleep(3)
        #
        # email: str = config.USERS[self.user_id]["email"]
        # username: str = config.USERS[self.user_id]["username"]
        # phone: str = config.USERS[self.user_id]["phone"]
        #
        # if not email:
        #     logger.error("not found email")
        #     raise ValueError
        #
        # email_input: ElementHandle = await self.wait_for_selector(selector="//input[@type='email']")
        # if not email_input:
        #     logger.error("not found email input")
        #     return
        #
        # await email_input.fill(email)
        # await asyncio.sleep(5)
        #
        # submit_button: ElementHandle = await self.wait_for_selector(selector="//input[@type='submit']")
        # if not submit_button:
        #     logger.error("not found submit button")
        #     return
        #
        # await submit_button.click()
        # await asyncio.sleep(10)
        #
        # try:
        #     captcha_frame: FrameLocator = self.page.frame_locator("#aa-challenge-whole-page-iframe")
        #     if captcha_frame:
        #         captcha_image: Locator = captcha_frame.locator("img[alt='captcha']")
        #         if not captcha_image:
        #             logger.error("not found captcha image")
        #             return
        #
        #         captcha_url: str = await captcha_image.get_attribute("src")
        #         if not captcha_url:
        #             logger.error("not found captcha url")
        #             return
        #
        #         captcha_solution: str = await solve_captcha(file=captcha_url)
        #
        #         if not captcha_solution:
        #             logger.error("not found captcha solution")
        #             return
        #
        #         captcha_input: Locator = captcha_frame.locator("//input[@id='aa_captcha_input']")
        #         if not captcha_input:
        #             logger.error("not found captcha input")
        #             return
        #
        #         await captcha_input.fill(captcha_solution)
        #         await asyncio.sleep(5)
        #
        #         submit_button: Locator = captcha_frame.locator("//input[@type='submit']")
        #         if not submit_button:
        #             logger.error("not found submit button")
        #             return
        #
        #         await submit_button.click()
        #         await asyncio.sleep(5)
        # except Exception as e:
        #     logger.error(e)
        #
        # if not username:
        #     logger.error("not found username")
        #     raise ValueError

        email: str = config.USERS[self.user_id]["email"]
        username: str = config.USERS[self.user_id]["username"]
        phone: str = config.USERS[self.user_id]["phone"]

        if not email:
            logger.error("not found email")
            raise ValueError

        login_button: ElementHandle = await self.wait_for_selector(selector="//strong[text()='Log in']")
        if login_button:
            await login_button.click()
            await asyncio.sleep(3)

            email_input: ElementHandle = await self.wait_for_selector(selector="//input[@type='email']")
            if not email_input:
                logger.error("not found email input")
                return

            await email_input.fill(email)
            await asyncio.sleep(5)

            submit_button: ElementHandle = await self.wait_for_selector(selector="//input[@type='submit']")
            if not submit_button:
                logger.error("not found submit button")
                return

            await submit_button.click()
            await asyncio.sleep(10)

            try:
                captcha_frame: FrameLocator = self.page.frame_locator("#aa-challenge-whole-page-iframe")
                if captcha_frame:
                    captcha_image: Locator = captcha_frame.locator("img[alt='captcha']")
                    if not captcha_image:
                        logger.error("not found captcha image")
                        return

                    captcha_url: str = await captcha_image.get_attribute("src")
                    if not captcha_url:
                        logger.error("not found captcha url")
                        return

                    captcha_solution: str = await solve_captcha(file=captcha_url)

                    if not captcha_solution:
                        logger.error("not found captcha solution")
                        return

                    captcha_input: Locator = captcha_frame.locator("//input[@id='aa_captcha_input']")
                    if not captcha_input:
                        logger.error("not found captcha input")
                        return

                    await captcha_input.fill(captcha_solution)
                    await asyncio.sleep(5)

                    submit_button: Locator = captcha_frame.locator("//input[@type='submit']")
                    if not submit_button:
                        logger.error("not found submit button")
                        return

                    await submit_button.click()
                    await asyncio.sleep(5)
            except Exception as e:
                logger.error(e)

            if not username:
                logger.error("not found username")
                raise ValueError

        password: str = config.get_password(username=username)

        if not password:
            logger.error("not found password")
            raise ValueError

        password_input: ElementHandle = await self.wait_for_selector(selector="//input[@type='password']")
        if not password_input:
            logger.error("not found password input")
            return

        await password_input.fill(password)
        await asyncio.sleep(5)

        submit_button: ElementHandle = await self.wait_for_selector(selector="//input[@type='submit']")
        if not submit_button:
            logger.error("not found submit button")
            return

        await submit_button.click()
        await asyncio.sleep(5)

        # tel_input: ElementHandle = await self.wait_for_selector(selector="//input[@type='tel']", save_screen=False)
        # if tel_input:
        #     if not phone:
        #         logger.error("not found phone")
        #         raise ValueError
        #
        #     otp_code: t.Optional[str] = None
        #     for _ in range(20):
        #         otp_code: str = await db.get_sms_code(phone=phone)
        #
        #         if otp_code:
        #             break
        #         else:
        #             await asyncio.sleep(30)
        #
        #     if not otp_code:
        #         logger.error("not found otp code")
        #         raise ValueError
        #
        #     await tel_input.fill(otp_code)
        #     await asyncio.sleep(3)
        #
        #     submit_button: ElementHandle = await self.wait_for_selector(selector="//input[@type='submit']")
        #     if not submit_button:
        #         logger.error("not found submit button")
        #         return
        #
        #     await submit_button.click()
        #     await asyncio.sleep(5)

        submit_button: ElementHandle = await self.wait_for_selector(selector="//a[@id='auth-get-new-otp-link']")
        if submit_button:
            await submit_button.click()
            await asyncio.sleep(15)

        radio_button: ElementHandle = await self.wait_for_selector(selector="(//input[@name='otpDeviceContext'])[1]")
        if not radio_button:
            logger.error("not found radio button")
            return

        await radio_button.click()
        await asyncio.sleep(15)

        for _ in range(10):
            submit_button: ElementHandle = await self.wait_for_selector(selector="//input[@id='auth-send-code']")
            if not submit_button:
                logger.error("not found submit button")
                return

            await submit_button.click()
            await asyncio.sleep(10)

            tel_input: ElementHandle = await self.wait_for_selector(selector="//input[@type='tel']")
            if tel_input:
                break

            await asyncio.sleep(30)

        otp_code = await generate_otp(user_id=self.user_id)
        await tel_input.fill(otp_code)
        await asyncio.sleep(2)

        submit_button: ElementHandle = await self.wait_for_selector(selector="//input[@type='submit']")
        if not submit_button:
            logger.error("not found submit button")
            return

        await submit_button.click()
        await asyncio.sleep(5)

        await self.run_js("close_popover.js")
        await asyncio.sleep(5)

        region_button: ElementHandle = await self.wait_for_selector(
            selector="//span[contains(text(), 'United States')]"
        )
        if region_button:
            await region_button.click()
            await asyncio.sleep(3)

            region_submit: ElementHandle = await self.wait_for_selector(
                selector="//button[contains(text(), 'Select account')]"
            )
            if region_submit:
                await region_submit.click()
                await asyncio.sleep(5)

        is_logged: bool = await self.is_logged()
        if is_logged is None:
            raise BrowserExceptions.PageError()
        elif is_logged:
            return True

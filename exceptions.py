class BrowserExceptions:
    class InitError(Exception):
        """Error initializing the browser"""
        def __init__(self, message="Failed to initialize browser"):
            super().__init__(message)

    class ConnectionError(Exception):
        """Error connecting to the browser, using CDP session"""
        def __init__(self, message="Failed to connect to browser via CDP"):
            super().__init__(message)

    class PageError(Exception):
        """Error opening a page in the browser"""
        def __init__(self, message="Failed to open page in browser"):
            super().__init__(message)

    class ElementNotFoundError(Exception):
        """Error finding an element's selector by playwright"""
        def __init__(self, element: str):
            super().__init__(f"Failed to find element - {element}")


class BColors:
    """Color codes for stdout"""
    HEADER = '\033[95m'
    GREEN = '\033[32m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'
    BLINKING = '\033[5m'
    YELLOW = '\033[33m'
    RED = '\033[31m'
    BLUE = '\033[34m'
    MAGENTA = '\033[35m'
    CYAN = '\033[36m'
    LIGHTGRAY = '\033[37m'
    DARKGRAY = '\033[90m'
    LIGHTRED = '\033[91m'
    LIGHTGREEN = '\033[92m'
    LIGHTYELLOW = '\033[93m'
    LIGHTBLUE = '\033[94m'
    LIGHTPINK = '\033[95m'
    LIGHTCYAN = '\033[96m'
    WHITE = '\033[97m'
    BLUEOK = LIGHTBLUE + "[OK]: " + ENDC
    GREENOK = GREEN + "[OK]: " + ENDC

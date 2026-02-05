BASE_URL = "https://star.tax.gov.ir/dashboard/home"

XPATHS = {
    "residegi": "//*[contains(text(), 'رسیدگی')]",
    "cycle_dashboard": "//*[contains(text(), 'داشبورد چرخه کار اظهارنامه')]",
    "by_province": "//*[contains(text(), 'تعداد اظهارنامه های در هر مرحله برحسب اداره کل')]",
    "khozestan": "//div[contains(@class,'navigable') and normalize-space()='استان خوزستان']",
    "by_office": "//*[contains(text(), 'تعداد اظهارنامه ها در هر مرحله برحسب اداره امور')]",
    "grid_cell": "//td[@data-kendo-grid-column-index='8']//div[contains(@class,'navigable')]",
    "final_details": "//*[contains(text(), 'جزئیات قطعی سازی اظهارنامه مودی')]",
}

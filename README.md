# VodiWalker Professional 27 — Ultimate Control Center

نسخه حرفه‌ای پنل مدیریت VodiWalker با تمرکز روی Railway، مدیریت Inbound/Client، ربات تلگرام، دسترسی ادمین‌ها و شخصی‌سازی ظاهر.

## ورود پنل
- Username پیش‌فرض: `admin`
- Password پیش‌فرض: `admin`
- بعد از ورود، Owner می‌تواند Username و Password حساب خودش را مستقیماً از Settings تغییر دهد.

برای محیط Production می‌توان مقادیر اولیه را با `ADMIN_USERNAME` و `ADMIN_PASSWORD` در Railway تعیین کرد.

## قابلیت‌های اصلی
- Inbound Studio حرفه‌ای با ظرفیت کاربر، تعداد خروجی کانفیگ و زمان دقیق انقضا
- Client Manager برای چند کاربر مستقل روی هر Inbound
- تشخیص LIVE / LINK-ONLY بر اساس هسته فعلی
- Railway TCP Proxy Auto configuration
- Message Center برای خطاها و رویدادها
- Appearance Studio با فونت، پوسته، رنگ، تراکم و اندازه متن
- مدیریت ادمین با Permission Matrix + تغییر نام کاربری و رمز حساب فعلی
- زبان انگلیسی واقعی برای رابط داشبورد با LTR و ترجمه‌ی عناصر پویا
- Bot Control Center و ویرایش متن‌های کلیدی ربات از داخل پنل
- حذف مسیر/دکمه‌ی تمدید اشتراک از رابط اشتراک و عدم ارائه‌ی renewal flow در ربات
- Sales Bot + Management Bot روی state مشترک پنل
- Healthcheck روی `/health`

## Railway
برنامه روی `0.0.0.0:$PORT` اجرا می‌شود. برای TCP Proxy، دامنه و پورت عمومی را از متغیرهای Railway/تنظیمات TCP Proxy دریافت کنید و پورت داخلی Relay را به TCP Proxy متصل کنید.

## اجرا
```bash
pip install -r requirements.txt
uvicorn main:app --host 0.0.0.0 --port ${PORT:-8000}
```

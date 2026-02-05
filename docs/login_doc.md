## 🔍 Problem Summary

I was scraping `tax.gov.ir` with Selenium (Firefox).
Every script run forced a **full login + captcha**, even though I was on the **same machine** and the site clearly used cookies/session tokens.

This caused:

- Unnecessary logins
- CAPTCHA triggers
- Fragile automation
- Slower runs

---

## ❌ Root Cause

1. Selenium was starting Firefox with a **temporary profile**
2. Temporary profiles **discard cookies & session data** on exit
3. The site relies on **ASP.NET session cookies**
4. Therefore, every run looked like a **brand-new browser**

Even though Firefox itself can stay logged in for weeks, Selenium wasn’t reusing that state.

---

## ✅ Solution Overview (What Fixed It)

### 1️⃣ Switched Selenium to a **persistent Firefox profile**

- Created a dedicated Firefox profile (`taxgov`)
- Logged in **once manually**
- Reused that profile in Selenium on every run

This preserves:

- Cookies
- LocalStorage
- Session tokens
- Browser fingerprint

➡️ Selenium now behaves like a real, returning user.

---

### 2️⃣ Added **login detection logic** instead of blind login

The site behavior:

| URL opened                       | Result             | Meaning        |
| -------------------------------- | ------------------ | -------------- |
| `/Page/Index` → stays            | ✅ Logged in       | Do nothing     |
| `/Page/Index` → redirects to `/` | ❌ Session expired | Login required |

So the fix was:

```text
Open /Page/Index
↓
Check current_url
↓
Only login if redirected
```

This avoids:

- Unnecessary credential submission
- CAPTCHA re-triggering
- Session invalidation

---

### 3️⃣ Combined both approaches safely

- **Persistent Firefox profile** → keeps session
- **URL-based login check** → detects expiration
- **Fallback login flow** → only when truly required

Result:

- Fast startup
- Minimal logins
- Stable long-running scraper

---

## 🧠 Final Architecture (Mental Model)

```text
init_driver()
 └─ Starts Firefox with persistent profile

login_codeghtesadi()
 ├─ Open /Page/Index
 ├─ If redirected → login + captcha
 └─ If not → already logged in

Scraping logic
```

---

## ⚠️ Important Constraints (Why This Works)

- Same machine
- Same Windows user
- Non-headless Firefox
- Only one Firefox instance at a time
- No clearing browser data

Violating these will break session persistence.

---

## ✅ Outcome

- ✔ No repeated logins
- ✔ CAPTCHA only when session truly expires
- ✔ Faster & more human-like automation
- ✔ Gov portal stays happy
- ✔ Code is deterministic and robust

---

## 📝 One-Line Memory Hook (TL;DR)

> **The bug was Selenium using a temporary Firefox profile.
> The fix was switching to a persistent profile + logging in only when `/Page/Index` redirects.**

If you ever come back to this code months later — that sentence alone will snap everything back into focus.

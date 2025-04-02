import requests
import time
import json
from concurrent.futures import ThreadPoolExecutor
import firebase_admin
from firebase_admin import credentials, db
import ast
import base64
import mysql.connector
from mysql.connector import OperationalError, DatabaseError
import traceback
import threading
import concurrent.futures
import re
import threading
import time
from datetime import datetime, timedelta
import uuid

# المتغيرات العامة
successful_requests_lock = threading.Lock()
successful_requests_counter = 0
is_completed = False
view_tracker = {}
SCRIPT_ID = str(uuid.uuid4())  # معرف فريد للسكربت

# إعدادات قاعدة البيانات
db_config = {
    'user': 'u151000592_smarttik',
    'password': 'Aa132132@@@',
    'host': 'srv1484.hstgr.io',
    'database': 'u151000592_smarttik'
}

def connect_to_db():
    global db_connection
    while True:
        try:
            db_connection = mysql.connector.connect(**db_config)
            print("تم الاتصال بقاعدة البيانات بنجاح.")
            break
        except DatabaseError as e:
            print(f"فشل الاتصال بقاعدة البيانات: {e}. المحاولة مرة أخرى بعد 5 ثوانٍ...")
            time.sleep(5)
connect_to_db()

def update_order_status(order_id, status, api_order_id='no_comment'):
    """تحديث حالة الطلب في قاعدة البيانات"""
    try:
        # التأكد من الاتصال بقاعدة البيانات
        if not db_connection.is_connected():
            connect_to_db()

        with db_connection.cursor() as cursor:
            # التحقق من وجود الطلب أولاً
            cursor.execute("SELECT id FROM orders WHERE id = %s", (order_id,))
            if not cursor.fetchone():
                print(f"❌ الطلب {order_id} غير موجود في قاعدة البيانات")
                return False

            # تحديث حالة الطلب
            query = """
                UPDATE orders 
                SET status = %s, api_order_id = %s, updated_at = NOW()
                WHERE id = %s
            """
            cursor.execute(query, (status, api_order_id, order_id))
            db_connection.commit()

            # التحقق من نجاح التحديث
            cursor.execute("SELECT status FROM orders WHERE id = %s", (order_id,))
            result = cursor.fetchone()
            if result and result[0] == status:
                print(f"✅ تم تحديث حالة الطلب {order_id} إلى {status}")
                return True
            else:
                print(f"❌ فشل تحديث حالة الطلب {order_id}")
                return False

    except mysql.connector.Error as err:
        print(f"❌ خطأ في قاعدة البيانات: {err}")
        try:
            connect_to_db()  # محاولة إعادة الاتصال
        except:
            pass
        return False
    except Exception as e:
        print(f"❌ خطأ غير متوقع: {e}")
        return False

# إعدادات Firebase
firebase_config = {
  "type": "service_account",
  "project_id": "myapp-572ea",
  "private_key_id": "346fad525609a8260e4ce4abf0c4e8470ff4a95f",
  "private_key": "-----BEGIN PRIVATE KEY-----\nMIIEvQIBADANBgkqhkiG9w0BAQEFAASCBKcwggSjAgEAAoIBAQDQHVAf8S3de9j9\nHJeUJUM2n2/VIbW4SiiCvV6cGeET2Aa26gajpbCDD04XTg16qQGtoOBOdnoN9/bY\ntarEN6BjfwU0qt7R/Er8rHTBcIAU0boEtCjmVdqSpTvsDBzxS6MpDVAVo5jGKTbX\nH94LYr83Pa/y8Y4QYgI+sX+XEZkdTJMfVUFhEyAlZ9faXSOHl6c5nB1X6IfA7q6w\nnhRKbcHKbVd1bCQ/C35hVqgfY4IDOQG+UtCNr2/K8haKtawdgIJJfG5lryTa7g32\np82ebWEwlj6kKO/cA9UjLNigrnbMJ1IguQaoPqgwsdEmBOV2ro9t5GG92rKBf8qy\nkUHkRVp9AgMBAAECggEAIPPTQQuGMKCwjfteAOYZi2eZZIzu4hxT58emWF88pVC6\nV3Ic1QKiPU5KFiSUu8xQ6LYlbicSUm3l1xCE1obcTYTKRTmdCHDDStjfr9VpYmKG\n6sHADCCh+EBTiZ+tYYORdSmXeaWqxg22kj+IgBMFpfCNkJEw5mUuZ9DhHAF8VggF\n1FgvzEBFBPrIgZ0F0Q2H11pEkx6f42Pq4k35tyxcsd9tWS/4NR07UU6Ho/I687hQ\nPGVH1joO31HUpvqMFi4dgRLbQub3K0aZ4t+jvZUO3qX00sGLI6wPX5pJa2sVCeEp\nnK/AYLVF7HuWM8w49/3yRnOEHPEp3JiPcU36mYeCfQKBgQDi7FtS8Ec3JJGR1OMT\njhcXeIGN9X8ewDj4TTsh5TZRAuVxDYODOoRKMbLS6blrids9rszTb3PoZ/liM4OA\noJgRJ9kvjqyhXh+St1qktKjtPjrNiTeMb8MRjOtM/ouks3BNP7j9LwVzbOH6D7G4\nRO6Ghq32BqESPubbrrZuNE28JwKBgQDqx/t7ayU7vXff1CYGpcOLBUOc2DsBfMFM\n8YN2GsLBRYq7Stb0nz9HwsAnvlbN+GjxQ13rROrnsFGwi8/iGgBOOAEhibaaOi3A\ntMRGnDwXSyxcJJRvQlV2Ah8Uu0Mm1rfFjg0DBflej9HYocYLqZ+cpEN1DeiQv6nh\nhiBZYswGuwKBgQCoHohCWDXKytL2ggyCLmE5SFRjgiBd/fe3LjDDGfg16LloGqT2\nkmH0PgHIdNSksYu2lUy20PnEk1OVx6iDoXdILoefzkpje0JVBnanVBlbOyGBqUl4\nSEzzMb4aWvPGYBKxiKZbHX0iWObKxtnaNWeqTxmPDrZCuEt9MmGCIEn/PQKBgBqg\nzeQElfd9mL/5JsrwOApncXCzkZWsmZvpdHiLtnUHNyHg7hpWiW3RJ8waKWw49WYP\ntvI9IctfUxSL9ur0+f3lGjO8k8pQOOo3Vl+PzrzxmLlmqLMpudmlifLm/knEZplw\ncAdcwRi5hRpl1rlx5pl0g/YdurfYFSNv+/FHZE6LAoGAFBCAyXOyQJX/hSvhWjal\nPaRl1tC7KvsEgzaJn/TPmqXshaiJIC7/c0cNUI79yUPjQqesRR7QJhmNPYHmXVkZ\nEP1KJm7dQazfrCq4hsHyJpzn5Dj5rrgRf3/iuqAnj8ZhCsqe0h5KmIINR01J//cY\nuwdM2ydiIvCVK74OCzs11Y8=\n-----END PRIVATE KEY-----\n",
  "client_email": "firebase-adminsdk-3yq47@myapp-572ea.iam.gserviceaccount.com",
  "client_id": "113250760604956137514",
  "auth_uri": "https://accounts.google.com/o_oauth2/auth",
  "token_uri": "https://oauth2.googleapis.com/token",
  "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
  "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/firebase-adminsdk-3yq47%40myapp-572ea.iam.gserviceaccount.com",
  "universe_domain": "googleapis.com"
}

# Check if Firebase is already initialized
if not firebase_admin._apps:
    cred = credentials.Certificate(firebase_config)
    firebase_admin.initialize_app(cred, {
        'databaseURL': 'https://myapp-572ea-default-rtdb.firebaseio.com/'
    })

# Reference to TikTok status in Firebase
tiktok_status_ref = db.reference('tiktok_status')

def check_tiktok_status():
    """التحقق من حالة TikTok"""
    try:
        status = tiktok_status_ref.get()
        return status.get('is_open3', False) if status else False
    except Exception as e:
        print(f"Error checking TikTok status: {str(e)}")
        return False

def convert_to_json(raw_text: str):
    try:
        # إذا كان النص عبارة عن قاموس، قم بإرجاعه مباشرة
        if isinstance(raw_text, dict):
            return raw_text

        # تحليل النص إلى كود Python
        parsed_text = ast.literal_eval(raw_text)

        # تحويل البيانات إلى JSON
        json_data = json.loads(json.dumps(parsed_text))

        return json_data

    except (json.JSONDecodeError, SyntaxError, ValueError) as e:
        return {"error": f"خطأ في تحويل النص إلى JSON: {e}"}

def decode_base64(encoded_data):
    try:
        return base64.b64decode(encoded_data).decode('utf-8')
    except Exception as e:
        print(f"خطأ في فك تشفير Base64: {e}")
        return None

def save_request(order_id, request_data):
    try:
        ref = db.reference('mobile_1_requests')
        ref.child(str(order_id)).set({
            **request_data,
            'connected_devices': 0,
            'quantity': 0,  # عداد الكمية الحالية
            'current_quantity': request_data.get('current_quantity', 1000),  # الكمية المطلوبة
            'timestamp': time.time()
        })
        print(f"تم حفظ الطلب {order_id} في Firebase")
        return True
    except Exception as e:
        print(f"خطأ في حفظ الطلب: {e}")
        return False

class ViewTracker:
    def __init__(self):
        self.last_plays = 0      # عدد المشاهدات قبل الـ 200 طلب
        self.current_plays = 0    # عدد المشاهدات بعد الـ 200 طلب
        self.request_counter = 0
        self.check_interval = 200

def get_video_stats(item_id, previous_views=None, order_id=None):
    """الحصول على إحصائيات الفيديو من Fast70App API"""
    for attempt in range(3):  # 3 محاولات
        try:
            url = f"https://fast70app.com/getvio.php?id={item_id}"

            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36',
                'Accept': 'application/json',
                'Accept-Language': 'en-US,en;q=0.9',
                'Connection': 'keep-alive'
            }

            if attempt > 0:
                print(f"محاولة {attempt + 1}/3 للحصول على الإحصائيات...")

            session = requests.Session()
            session.verify = False
            response = session.get(url, headers=headers, timeout=30)

            if response.status_code == 200:
                # التحقق من أن المحتوى نص وليس فارغاً
                content = response.text.strip()
                if not content:
                    print("❌ المحتوى فارغ")
                    if attempt < 2:
                        time.sleep(1)
                    continue

                try:
                    data = response.json()
                    print(f"البيانات المستلمة: {data}")  # طباعة البيانات للتشخيص

                    if isinstance(data, dict) and data.get('success'):
                        try:
                            current_views = int(str(data.get('viewCount', '0')).replace(',', ''))
                            if current_views == 0:
                                print("❌ عدد المشاهدات صفر، جاري إعادة المحاولة...")
                                if attempt < 2:
                                    time.sleep(1)
                                continue

                            # حفظ المشاهدات في Firebase
                            if order_id:
                                ref = db.reference(f'mobile_1_requests/{order_id}')
                                ref.update({
                                    'viewCount': current_views,
                                    'last_check_time': int(time.time())
                                })

                            print(f"إحصائيات الفيديو {item_id}:")
                            print(f"- المشاهدات: {current_views:,}")
                            print(f"- الإعجابات: {data.get('likeCount', 0):,}")
                            print(f"- التعليقات: {data.get('commentCount', 0):,}")
                            print(f"- المشاركات: {data.get('shareCount', 0):,}")

                            if previous_views is not None:
                                difference = current_views - previous_views
                                print(f"التغيير في المشاهدات: {difference:+,}")
                                if difference > 0:
                                    print("✅ زادت المشاهدات")
                                elif difference < 0:
                                    print("⚠️ نقصت المشاهدات")
                                else:
                                    print("ℹ️ لم تتغير المشاهدات")

                            return current_views
                        except (ValueError, TypeError) as e:
                            print(f"❌ خطأ في تحويل عدد المشاهدات: {e}")
                            print(f"قيمة viewCount: {data.get('viewCount')}")
                            if attempt < 2:
                                time.sleep(1)
                            continue
                    else:
                        print(f"❌ خطأ في البيانات: {data}")
                        print(f"المحتوى المستلم: {content}")
                except json.JSONDecodeError as je:
                    print(f"❌ خطأ في تحليل JSON (محاولة {attempt + 1}/3)")
                    print(f"المحتوى المستلم: {content[:200]}")  # عرض أول 200 حرف فقط
                    if attempt < 2:
                        time.sleep(1)
                    continue
                except Exception as e:
                    print(f"❌ خطأ في معالجة البيانات: {str(e)}")
                    print(f"المحتوى المستلم: {content}")
                    if attempt < 2:
                        time.sleep(1)
                    continue
            else:
                print(f"❌ خطأ في الطلب: {response.status_code}")
                print(f"محتوى الاستجابة: {response.text[:200]}")
                if attempt < 2:
                    time.sleep(1)
                continue

        except Exception as e:
            print(f"❌ خطأ في الحصول على إحصائيات الفيديو (محاولة {attempt + 1}/3): {e}")
            if attempt < 2:
                time.sleep(1)
            continue

    print("❌ فشلت جميع المحاولات للحصول على الإحصائيات")
    return 0

def verify_order(ref, order_id, first_check=False):
    """التحقق من وجود الطلب وشروطه"""
    try:
        # الحصول على بيانات الطلب
        order_data = ref.get()
        if not order_data:
            if first_check:
                print(f"❌ الطلب {order_id} غير موجود")
            return False

        # التحقق من المعرفات المحظورة
        blocked_scripts = order_data.get('blocked_scripts', [])
        if SCRIPT_ID in blocked_scripts:
            if first_check:
                print(f"❌ هذا السكربت محظور من الطلب {order_id}")
            return False

        # التحقق من الكمية
        current_quantity = order_data.get('quantity', 0)
        target_quantity = order_data.get('target_quantity', 1000)

        if first_check:
            print(f"\nمعلومات الطلب {order_id}:")
            print(f"- الكمية المنفذة: {current_quantity:,}")
            print(f"- الكمية المطلوبة: {target_quantity:,}")

            # التحقق من صحة البيانات
            if target_quantity <= 0:
                print("❌ الكمية المطلوبة غير صالحة")
                ref.delete()
                return False

            if current_quantity > target_quantity:
                print("⚠️ تصحيح الكمية المنفذة لتتناسب مع الكمية المطلوبة")
                ref.update({'quantity': 0})
                current_quantity = 0

        if current_quantity >= target_quantity:
            if first_check:
                print(f"✅ الطلب {order_id} مكتمل")
            ref.delete()
            return False

        return True
    except Exception as e:
        print(f"خطأ في التحقق من الطلب: {e}")
        return False

def check_views_difference(old_views, new_views, order_ref, item_id):
    """التحقق من الفرق في المشاهدات"""
    try:
        difference = new_views - old_views
        print(f"التغيير في المشاهدات: {'+' if difference >= 0 else ''}{difference}")

        if difference > 0:
            print("✅ زادت المشاهدات")
        elif difference < 0:
            print("❌ نقصت المشاهدات")
        else:
            print("⚠️ لم تتغير المشاهدات")

        print(f"الفرق في المشاهدات: {'+' if difference >= 0 else ''}{difference}\n")

        if difference <= 5:  # إذا كان الفرق في المشاهدات قليل
            print("⚠️ تم إيقاف الطلب لأن الفرق في المشاهدات قليل جداً " +
                  f"({'+' if difference >= 0 else ''}{difference})")

            try:
                # الحصول على بيانات الطلب
                order_data = order_ref.get()
                if not order_data:
                    return False

                # إعداد قائمة السكربتات المحظورة
                blocked_scripts = order_data.get('blocked_scripts', [])
                if not isinstance(blocked_scripts, list):
                    blocked_scripts = []

                # إضافة المعرف للقائمة السوداء
                blocked_scripts.append(SCRIPT_ID)
                
                # تحديث البيانات وإعادة تعيين عدد الأجهزة المتصلة إلى 0
                order_ref.update({
                    'quantity': order_data.get('quantity', 0),
                    'viewCount': new_views,
                    'last_check_time': int(time.time()),
                    'status': 'stopped',
                    'stopped_at': int(time.time()),
                    'views_difference': difference,
                    'blocked_scripts': blocked_scripts,
                    'connected_devices': 0,  # إعادة تعيين عدد الأجهزة المتصلة إلى 0
                    'last_device_update': int(time.time())
                })
                print("✅ تم تحديث قائمة المعرفات المحظورة وإعادة تعيين عدد الأجهزة المتصلة")

            except Exception as e:
                print(f"❌ خطأ في تحديث قائمة السكربتات المحظورة: {str(e)}")
            return False

        return True

    except Exception as e:
        print(f"❌ خطأ في التحقق من الفرق في المشاهدات: {str(e)}")
        return False

class ViewTracker:
    def __init__(self):
        self.last_plays = 0      # عدد المشاهدات قبل الـ 200 طلب
        self.current_plays = 0    # عدد المشاهدات بعد الـ 200 طلب
        self.request_counter = 0
        self.check_interval = 200

def process_request_batch(request_data, order_id, ref):
    """معالجة مجموعة من الطلبات بشكل متوازي"""
    global successful_requests_counter, is_completed

    try:
        # محاولة الحصول على القفل
        if not device_lock.acquire(timeout=2):  # انتظار ثانيتين كحد أقصى
            print("⚠️ لم نتمكن من الحصول على قفل الجهاز")
            return

        try:
            # الحصول على بيانات الطلب
            order_data = ref.get()
            if not order_data:
                print("❌ لا يمكن العثور على بيانات الطلب")
                return

            # التحقق من المعرفات المحظورة أولاً
            blocked_scripts = order_data.get('blocked_scripts', [])
            if SCRIPT_ID in blocked_scripts:
                print(f"❌ هذا السكربت محظور من الطلب {order_id}")
                return

            # التحقق من عدد الأجهزة المتصلة
            connected_devices = order_data.get('connected_devices', 0)
            if connected_devices != 0:
                print(f"⚠️ هذا الطلب قيد التنفيذ من قبل جهاز آخر")
                return

            # تحديث عدد الأجهزة المتصلة إلى 1 فقط إذا لم يكن محظوراً ولا يوجد أجهزة متصلة
            ref.update({
                'connected_devices': 1,
                'last_device_update': int(time.time()),
                'device_id': threading.get_ident()
            })
            print("✅ تم الاتصال بالطلب بنجاح")

        finally:
            device_lock.release()  # تحرير القفل

        # استخراج معرف الفيديو من body
        encoded_body = request_data.get('body', None)
        body = decode_base64(encoded_body) if encoded_body else None

        if not body:
            print("❌ لم يتم العثور على body")
            reset_connected_devices(ref)
            return

        item_id = extract_item_id(body)
        if not item_id:
            print("❌ لم يتم العثور على معرف الفيديو في body")
            reset_connected_devices(ref)
            return

        # جلب إحصائيات الفيديو الأولية
        initial_views = 0
        for _ in range(3):  # 3 محاولات
            initial_views = get_video_stats(item_id, order_id=order_id)
            if initial_views > 0:
                break
            time.sleep(1)

        if initial_views == 0:
            print("❌ لم نتمكن من الحصول على عدد المشاهدات بعد 3 محاولات")
            reset_connected_devices(ref)
            return

        current_views = initial_views

        # الحصول على الكمية المطلوبة والحالية
        target_quantity = order_data.get('target_quantity', 0)
        current_quantity = order_data.get('quantity', 0)  # نستخدم الكمية المخزنة في Firebase

        print(f"\nمعلومات الطلب:")
        print(f"- الكمية الحالية في Firebase: {current_quantity:,}")
        print(f"- المشاهدات الأولية في الفيديو: {initial_views:,}")
        print(f"- الكمية المطلوبة: {target_quantity:,}")

        # حساب العدد المتبقي للتنفيذ
        remaining = target_quantity - current_quantity
        if remaining <= 0:
            print(f"✅ الطلب {order_id} مكتمل مسبقاً")
            ref.delete()
            reset_connected_devices(ref)
            return

        print(f"\n⏳ جاري تنفيذ {remaining:,} طلب متبقي...")

        # تحديث الكمية الحالية في Firebase
        ref.update({
            'quantity': current_quantity,
            'viewCount': initial_views,
            'last_check_time': int(time.time()),
            'target_quantity': target_quantity
        })

        batch_size = 200  # تحديث كل 200 طلب للتحقق من المشاهدات
        last_check_views = initial_views

        with ThreadPoolExecutor(max_workers=50) as executor:
            futures = []
            pending_futures = set()
            max_concurrent = 50

            for i in range(remaining):
                if is_completed:
                    break

                while len(pending_futures) >= max_concurrent:
                    done, pending_futures = concurrent.futures.wait(
                        pending_futures,
                        timeout=0.1,
                        return_when=concurrent.futures.FIRST_COMPLETED
                    )

                    for future in done:
                        try:
                            if future.result():
                                with successful_requests_lock:
                                    successful_requests_counter += 1
                                    current_count = current_quantity + successful_requests_counter
                                    percentage = (current_count * 100) // target_quantity
                                    print(f"\rالتقدم: {current_count:,}/{target_quantity:,} ({percentage}%)", end='', flush=True)

                                    if successful_requests_counter % batch_size == 0:
                                        try:
                                            # تحديث عدد المشاهدات كل 200 طلب
                                            print("\n⏳ جاري التحقق من عدد المشاهدات...")
                                            current_views = get_video_stats(item_id, previous_views=last_check_views)

                                            if current_views > 0:
                                                # حساب الفرق في المشاهدات
                                                if not check_views_difference(last_check_views, current_views, ref, item_id):
                                                    reset_connected_devices(ref)
                                                    return

                                                last_check_views = current_views

                                            current_data = ref.get()
                                            if current_data is not None:
                                                new_quantity = current_quantity + successful_requests_counter
                                                ref.update({
                                                    'quantity': new_quantity,
                                                    'viewCount': current_views,
                                                    'last_check_time': int(time.time()),
                                                    'target_quantity': target_quantity
                                                })
                                                print(f"\n✅ تم تحديث الكمية في Firebase إلى: {new_quantity:,}")
                                                print(f"✅ عدد المشاهدات الحالي: {current_views:,}")

                                                if new_quantity >= target_quantity:
                                                    print(f"\n✅ تم اكتمال الطلب {order_id} بنجاح!")
                                                    ref.delete()
                                                    update_order_status(order_id, 'completed')  # تحديث حالة الطلب إلى مكتمل
                                                    is_completed = True
                                                    return
                                        except Exception as e:
                                            print(f"\nخطأ في تحديث الكمية: {str(e)}")
                        except Exception as e:
                            if "timeout" not in str(e).lower():
                                print(f"\nخطأ في تنفيذ الطلب: {str(e)}")

                if not is_completed:
                    future = executor.submit(send_single_request, request_data, order_id, i, ref)
                    pending_futures.add(future)

            # معالجة الطلبات المتبقية
            if pending_futures:
                for future in concurrent.futures.as_completed(pending_futures):
                    try:
                        if future.result():
                            with successful_requests_lock:
                                successful_requests_counter += 1
                                current_count = current_quantity + successful_requests_counter
                                percentage = (current_count * 100) // target_quantity
                                print(f"\rالتقدم: {current_count:,}/{target_quantity:,} ({percentage}%)", end='', flush=True)

                                if successful_requests_counter % batch_size == 0:
                                    try:
                                        # تحديث عدد المشاهدات كل 200 طلب
                                        print("\n⏳ جاري التحقق من عدد المشاهدات...")
                                        current_views = get_video_stats(item_id, previous_views=last_check_views)

                                        if current_views > 0:
                                            # حساب الفرق في المشاهدات
                                            if not check_views_difference(last_check_views, current_views, ref, item_id):
                                                reset_connected_devices(ref)
                                                return

                                            last_check_views = current_views

                                        current_data = ref.get()
                                        if current_data is not None:
                                            new_quantity = current_quantity + successful_requests_counter
                                            ref.update({
                                                'quantity': new_quantity,
                                                'viewCount': current_views,
                                                'last_check_time': int(time.time()),
                                                'target_quantity': target_quantity
                                            })
                                            print(f"\n✅ تم تحديث الكمية في Firebase إلى: {new_quantity:,}")
                                            print(f"✅ عدد المشاهدات الحالي: {current_views:,}")

                                            if new_quantity >= target_quantity:
                                                print(f"\n✅ تم اكتمال الطلب {order_id} بنجاح!")
                                                ref.delete()
                                                update_order_status(order_id, 'completed')  # تحديث حالة الطلب إلى مكتمل
                                                is_completed = True
                                                return
                                    except Exception as e:
                                        print(f"\nخطأ في تحديث الكمية: {str(e)}")
                    except Exception as e:
                        if "timeout" not in str(e).lower():
                            print(f"\nخطأ في تنفيذ الطلب: {str(e)}")

            # التحديث النهائي
            if not is_completed and successful_requests_counter > 0:
                try:
                    with successful_requests_lock:
                        # تحديث نهائي لعدد المشاهدات
                        print("\n⏳ جاري التحقق النهائي من عدد المشاهدات...")
                        final_views = get_video_stats(item_id, previous_views=last_check_views)

                        if final_views > 0:
                            # حساب الفرق النهائي في المشاهدات
                            final_difference = final_views - last_check_views
                            print(f"الفرق النهائي في المشاهدات: {final_difference:+,}")

                            # تحديث البيانات النهائية
                            new_quantity = current_quantity + successful_requests_counter
                            ref.update({
                                'quantity': new_quantity,
                                'viewCount': final_views,
                                'last_check_time': int(time.time()),
                                'target_quantity': target_quantity,
                                'final_views_difference': final_difference
                            })
                            percentage = (new_quantity * 100) // target_quantity
                            print(f"\n✅ النتيجة النهائية: {new_quantity:,}/{target_quantity:,} ({percentage}%)")
                            print(f"✅ عدد المشاهدات النهائي: {final_views:,}")

                        if new_quantity >= target_quantity:
                            print(f"✅ تم اكتمال الطلب {order_id} بنجاح!")
                            ref.delete()
                            update_order_status(order_id, 'completed')  # تحديث حالة الطلب إلى مكتمل
                            is_completed = True
                except Exception as e:
                    print(f"\nخطأ في التحديث النهائي للكمية: {str(e)}")
    except Exception as e:
        print(f"خطأ في معالجة الطلب: {str(e)}")
        # إعادة تصفير الأجهزة المتصلة في حالة حدوث خطأ
        try:
            reset_connected_devices(ref)
        except:
            pass
        return

def send_single_request(request_data, order_id, i, ref):
    try:
        # التحقق من وجود الطلب والشروط
        order_data = ref.get()
        if not order_data:
            return False

        # إضافة تأخير ثابت بين الطلبات
        time.sleep(0.003)  # تأخير 3 مللي ثانية

        # التحقق من حالة TikTok
        while check_tiktok_status():
            print("TikTok is open, waiting...")
            time.sleep(1)

        # معالجة headers
        headers = request_data.get('headers', '{}')
        headers_json = convert_to_json(headers) if isinstance(headers, str) else headers

        if not isinstance(headers_json, dict):
            print(f"Headers format is invalid for request ID {order_id}")
            return False

        # فك تشفير body من Base64
        encoded_body = request_data.get('body', None)
        body = decode_base64(encoded_body) if encoded_body else None

        # إرسال الطلب
        response = requests.request(
            method=request_data['method'],
            url=request_data['url'],
            headers=headers_json,
            data=body,
            verify=False,
            timeout=5
        )

        return response.status_code == 200
    except requests.exceptions.Timeout:
        print(f"⚠️ انتهت مهلة الطلب {i+1}")
        return False
    except Exception as e:
        print(f"خطأ في الطلب {i+1}: {str(e)}")
        return False

class RequestCounter:
    def __init__(self):
        self.count = 0
        self._lock = threading.Lock()

    def increment(self):
        with self._lock:
            self.count += 1
            return self.count

def execute_request(request_data, start_count=None):
    try:
        # الحصول على البيانات المطلوبة
        order_id = request_data.get('order_id')
        if not order_id:
            return False

        ref = db.reference(f'mobile_1_requests/{order_id}')
        current_data = ref.get()
        if not current_data:
            return False

        try:
            current_quantity = current_data.get('quantity', 0)
            target_quantity = current_data.get('target_quantity', 1000)

            remaining_quantity = target_quantity - current_quantity
            if remaining_quantity <= 0:
                print(f"✅ تم اكتمال الطلب مسبقاً. الكمية الحالية: {current_quantity}, الكمية المطلوبة: {target_quantity}")
                ref.delete()
                update_order_status(order_id, 'completed', 'no_comment')
                return True

            print(f"المتبقي للتنفيذ: {remaining_quantity} (الحالي: {current_quantity}, المطلوب: {target_quantity})")

            if start_count is None:
                start_count = remaining_quantity

            successful_requests = current_quantity

            encoded_body = request_data.get('body', None)
            body = decode_base64(encoded_body) if encoded_body else None
            item_id = extract_item_id(body) if body else None

            if not item_id:
                print("❌ لم يتم العثور على معرف الفيديو")
                return False

            pause_duration = 120  # مدة التوقف بالثواني
            batch_size = 200  # حجم الدفعة = 200
            same_views_count = 0  # عداد ثبات المشاهدات
            max_same_views = 3    # الحد الأقصى لثبات المشاهدات
            check_views_interval = 200  # فحص المشاهدات كل 200 طلب

            # إنشاء عداد مشترك للطلبات
            request_counter = RequestCounter()

            # فحص المشاهدات الأولي
            initial_views = get_video_stats(item_id, order_id=order_id)
            if initial_views == 0:
                print("❌ لم نتمكن من الحصول على عدد المشاهدات الأولي")
                return False

            print(f"عدد المشاهدات الأولي: {initial_views:,}")
            last_views = initial_views

            while remaining_quantity > 0:
                current_batch_size = min(batch_size, remaining_quantity)
                successful_in_batch = 0

                # إرسال الطلبات
                with ThreadPoolExecutor(max_workers=5) as executor:
                    futures = []
                    for i in range(current_batch_size):
                        futures.append(executor.submit(send_single_request, request_data, order_id, i, ref))

                    for future in futures:
                        if future.result():
                            successful_in_batch += 1
                            current_count = request_counter.increment()

                            # فحص المشاهدات كل 200 طلب
                            if current_count % check_views_interval == 0:
                                print(f"\nفحص المشاهدات بعد {current_count:,} طلب...")
                                current_views = get_video_stats(item_id, previous_views=last_views, order_id=order_id)

                                if current_views == 0:
                                    print("❌ فشل في الحصول على المشاهدات")
                                    time.sleep(5)
                                    continue

                                views_increase = current_views - last_views
                                print(f"عدد المشاهدات الحالي: {current_views:,}")
                                print(f"الزيادة في المشاهدات: {views_increase:+,}")

                                # التحقق من ثبات المشاهدات
                                if current_views == last_views:
                                    same_views_count += 1
                                    print(f"⚠️ لم تتغير المشاهدات ({same_views_count}/{max_same_views})")
                                    if same_views_count >= max_same_views:
                                        print("❌ توقف تزايد المشاهدات")
                                        print(f"❌ توقف مؤقت لمدة {pause_duration} ثانية")
                                        successful_requests += successful_in_batch
                                        ref.update({
                                            'connected_devices': 0,
                                            'quantity': successful_requests
                                        })
                                        time.sleep(pause_duration)
                                        return False
                                elif views_increase < 100:
                                    print(f"⚠️ زيادة المشاهدات أقل من 100 ({views_increase})")
                                    same_views_count += 1
                                    if same_views_count >= max_same_views:
                                        print("❌ توقف بسبب قلة الزيادة في المشاهدات")
                                        print(f"❌ توقف مؤقت لمدة {pause_duration} ثانية")
                                        successful_requests += successful_in_batch
                                        ref.update({
                                            'connected_devices': 0,
                                            'quantity': successful_requests
                                        })
                                        time.sleep(pause_duration)
                                        return False
                                else:
                                    same_views_count = 0
                                    last_views = current_views

                # تحديث الإحصائيات للدفعة الناجحة
                successful_requests += successful_in_batch
                remaining_quantity -= current_batch_size

                ref.update({
                    'quantity': successful_requests,
                    'connected_devices': 5
                })

                print(f"✅ تم تنفيذ {successful_in_batch} طلب في هذه الدفعة")
                print(f"✅ إجمالي الطلبات الناجحة: {successful_requests}")
                print(f"المتبقي: {remaining_quantity}")

            print(f"✅ تم تنفيذ جميع الطلبات بنجاح (الإجمالي: {successful_requests})")

            if successful_requests >= target_quantity:
                ref.delete()
                update_order_status(order_id, 'completed', 'no_comment')
                print(f"✅ تم اكتمال وحذف الطلب {order_id}")

            return True

        except Exception as e:
            raise e

    except Exception as e:
        print(f"❌ خطأ في تنفيذ الطلب {request_data.get('order_id', 'Unknown')}: {str(e)}")
        return False

def process_request(order_id, request_data):
    try:
        # إعادة تعيين المتغيرات العامة
        global successful_requests_counter, is_completed
        successful_requests_counter = 0
        is_completed = False

        ref = db.reference(f'mobile_1_requests/{order_id}')

        # التحقق من وجود الطلب وجميع المتغيرات المطلوبة
        if not verify_order(ref, order_id, first_check=True):
            print(f"❌ الطلب {order_id} غير موجود أو غير مكتمل")
            return False

        # الحصول على البيانات المطلوبة
        order_data = ref.get()
        if not order_data:
            print(f"❌ لا يمكن العثور على بيانات الطلب {order_id}")
            return False

        # التأكد من وجود الكمية المطلوبة
        target_quantity = order_data.get('target_quantity', 0)
        if target_quantity <= 0:
            print("❌ الكمية المطلوبة غير صالحة")
            ref.delete()
            return False

        # معالجة الطلبات
        return process_request_batch(request_data, order_id, ref)

    except Exception as e:
        print(f"خطأ في معالجة الطلب: {str(e)}")
        return False

def check_request_exists(requests_ref, order_id):
    """التحقق من وجود الطلب وجميع المتغيرات المطلوبة"""
    try:
        data = requests_ref.child(order_id).get()
        if not data:
            return False

        # التحقق من وجود جميع المتغيرات المطلوبة
        required_fields = ['method', 'url', 'headers', 'body']
        for field in required_fields:
            if field not in data:
                print(f"تخطي الطلب {order_id}: {field} غير موجود")
                return False
        return True
    except Exception as e:
        print(f"خطأ في التحقق من الطلب {order_id}: {str(e)}")
        return False

def update_connected_devices(requests_ref, order_id, increment=True):
    """تحديث عدد الأجهزة المتصلة مع التحقق من وجود الطلب"""
    try:
        # التحقق أولاً من وجود الطلب
        if not verify_order(requests_ref, order_id):
            print(f"لا يمكن تحديث connected_devices: الطلب {order_id} غير موجود")
            return False

        # جلب البيانات الحالية
        current_data = requests_ref.child(order_id).get()
        if not current_data:
            return False

        current_connected = current_data.get('connected_devices', 0)
        new_value = current_connected + (1 if increment else -1)

        # تحقق مرة أخرى قبل التحديث
        if not verify_order(requests_ref, order_id):
            print(f"لا يمكن تحديث connected_devices: الطلب {order_id} تم حذفه")
            return False

        requests_ref.child(order_id).update({'connected_devices': new_value})
        action = "زيادة" if increment else "تخفيض"
        print(f"تم {action} عدد الأجهزة المتصلة للطلب {order_id} إلى {new_value}")
        return True
    except Exception as e:
        print(f"خطأ في تحديث connected_devices للطلب {order_id}: {str(e)}")
        return False

def extract_item_id(url):
    """استخراج معرف العنصر من الرابط"""
    try:
        # البحث عن النمط item_id= في الرابط
        import re
        match = re.search(r'item_id=(\d+)', url)
        if match:
            return match.group(1)
        return None
    except Exception as e:
        print(f"خطأ في استخراج معرف العنصر: {str(e)}")
        return None

def track_view(item_id):
    """تتبع عدد المشاهدات لكل عنصر"""
    global view_tracker
    with successful_requests_lock:  # استخدام نفس القفل للتزامن
        if item_id not in view_tracker:
            view_tracker[item_id] = 0
        view_tracker[item_id] += 1
        return view_tracker[item_id]

def reset_connected_devices(ref):
    """إعادة تعيين عدد الأجهزة المتصلة"""
    try:
        # الحصول على بيانات الطلب
        order_data = ref.get()
        if not order_data:
            return

        # التحقق من المعرفات المحظورة أولاً
        if SCRIPT_ID in order_data.get('blocked_scripts', []):
            return

        # إعادة تعيين عدد الأجهزة المتصلة إلى 0
        ref.update({
            'connected_devices': 0,
            'last_device_update': int(time.time()),
            'device_id': None
        })
        print("✅ تم إعادة تعيين حالة الاتصال")

    except Exception as e:
        print(f"❌ خطأ في إعادة تعيين عدد الأجهزة المتصلة: {str(e)}")

def update_device_status(ref):
    """تحديث حالة الجهاز كل دقيقة"""
    try:
        # التحقق من المعرفات المحظورة أولاً
        order_data = ref.get()
        if order_data and SCRIPT_ID not in order_data.get('blocked_scripts', []):
            ref.update({
                'last_device_update': int(time.time())
            })
    except Exception as e:
        print(f"خطأ في تحديث حالة الجهاز: {str(e)}")

def main():
    current_order_id = None  # تخزين معرف الطلب الحالي
    requests_ref = db.reference('mobile_1_requests')

    while True:
        try:
            # الحصول على جميع الطلبات من Firebase
            firebase_data = requests_ref.get()
            if not firebase_data:
                print("لا توجد طلبات في Firebase")
                time.sleep(5)
                continue

            # إذا كان لدينا طلب حالي، نحاول استئنافه
            if current_order_id:
                current_data = requests_ref.child(current_order_id).get()
                if current_data:
                    # التحقق من حالة الطلب قبل الاستئناف
                    if SCRIPT_ID not in current_data.get('blocked_scripts', []) and current_data.get('connected_devices', 0) == 0:
                        print(f"استئناف العمل على الطلب {current_order_id}")
                        request_data = current_data
                        request_data['order_id'] = current_order_id
                        process_request(current_order_id, request_data)
                current_order_id = None

            # البحث عن طلب جديد
            available_requests = []
            for order_id, request_data in firebase_data.items():
                # التحقق من وجود البيانات الأساسية
                required_fields = ['method', 'url', 'headers']
                if not all(field in request_data for field in required_fields):
                    print(f"تخطي الطلب {order_id}: البيانات الأساسية غير مكتملة")
                    continue

                # تخطي الطلبات المحظورة
                if SCRIPT_ID in request_data.get('blocked_scripts', []):
                    continue

                # التحقق من عدد الأجهزة المتصلة - يجب أن يكون 0
                connected_devices = request_data.get('connected_devices', 0)
                if connected_devices != 0:
                    print(f"تخطي الطلب {order_id}: يوجد {connected_devices} جهاز متصل")
                    continue

                # التحقق من الكمية
                current_quantity = request_data.get('current_quantity', 0)
                target_quantity = request_data.get('target_quantity', current_quantity)

                # تحديث target_quantity إذا لم تكن موجودة
                if 'target_quantity' not in request_data and current_quantity > 0:
                    requests_ref.child(order_id).update({
                        'target_quantity': current_quantity
                    })

                available_requests.append((order_id, request_data))

            if not available_requests:
                print("لا توجد طلبات متاحة للتنفيذ...")
                time.sleep(5)
                continue

            # اختيار أول طلب متاح
            order_id, request_data = available_requests[0]

            # التحقق النهائي من حالة الاتصال قبل البدء
            final_check = requests_ref.child(order_id).get()
            if not final_check or final_check.get('connected_devices', 0) != 0 or SCRIPT_ID in final_check.get('blocked_scripts', []):
                print(f"⚠️ الطلب {order_id} غير متاح للتنفيذ")
                continue

            print(f"بدء تنفيذ الطلب {order_id}")
            current_order_id = order_id
            process_request(order_id, request_data)
            current_order_id = None

        except Exception as e:
            print(f"خطأ عام: {str(e)}")
            if current_order_id:
                try:
                    ref = requests_ref.child(current_order_id)
                    reset_connected_devices(ref)
                except:
                    pass
            time.sleep(5)

if __name__ == "__main__":
    device_lock = threading.Lock()
    main()

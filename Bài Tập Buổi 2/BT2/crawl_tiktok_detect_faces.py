from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import os
import cv2
from tiktok_scraper import scrape_trending

# Config
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# DAG định nghĩa
with DAG(
    'crawl_tiktok_detect_faces',
    default_args=default_args,
    description='Crawl videos and detect faces',
    schedule_interval='0 9 * * *',  # Chạy mỗi ngày vào 9 giờ sáng
    start_date=datetime(2024, 12, 17),
    catchup=False,
) as dag:

    # Tạo folder theo ngày
    def create_daily_folder():
        date_folder = datetime.now().strftime("%d_%m_%y")
        os.makedirs(f"data/{date_folder}", exist_ok=True)
        return f"data/{date_folder}"

    # Crawl videos
    def crawl_videos(**kwargs):
        folder = kwargs['ti'].xcom_pull(task_ids='create_folder')
        videos = scrape_trending(5)  # Crawl 5 videos trending
        for idx, video in enumerate(videos):
            video_path = os.path.join(folder, f"video_{idx + 1}.mp4")
            with open(video_path, 'wb') as f:
                f.write(video['video_content'])
        return folder

    # Detect faces in videos
    def detect_faces(**kwargs):
        folder = kwargs['ti'].xcom_pull(task_ids='crawl_videos')
        face_cascade = cv2.CascadeClassifier(cv2.data.haarcascades + 'haarcascade_frontalface_default.xml')

        for video_file in os.listdir(folder):
            if video_file.endswith(".mp4"):
                video_path = os.path.join(folder, video_file)
                cap = cv2.VideoCapture(video_path)
                fps = int(cap.get(cv2.CAP_PROP_FPS))

                count = 0
                frame_no = 0
                while cap.isOpened():
                    ret, frame = cap.read()
                    if not ret:
                        break

                    if frame_no % (fps // 5) == 0:  # Mỗi 5 FPS
                        gray = cv2.cvtColor(frame, cv2.COLOR_BGR2GRAY)
                        faces = face_cascade.detectMultiScale(gray, scaleFactor=1.1, minNeighbors=5)
                        for (x, y, w, h) in faces:
                            count += 1
                            face_frame = frame[y:y + h, x:x + w]
                            face_path = os.path.join(folder, f"face_{count}.jpg")
                            cv2.imwrite(face_path, face_frame)

                    frame_no += 1
                cap.release()

    # Task 1: Tạo folder theo ngày
    create_folder = PythonOperator(
        task_id='create_folder',
        python_callable=create_daily_folder,
    )

    # Task 2: Crawl videos
    crawl_videos_task = PythonOperator(
        task_id='crawl_videos',
        python_callable=crawl_videos,
        provide_context=True,
    )

    # Task 3: Detect faces
    detect_faces_task = PythonOperator(
        task_id='detect_faces',
        python_callable=detect_faces,
        provide_context=True,
    )

    # DAG workflow
    create_folder >> crawl_videos_task >> detect_faces_task

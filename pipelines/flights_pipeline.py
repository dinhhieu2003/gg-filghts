import pandas as pd
import json
import sys
import os
from datetime import datetime

root_dir = os.path.dirname(os.path.dirname(__file__))
sys.path.insert(0, root_dir)

from etls.flights_etl import extract_flights_information, transform_to_csv

# Danh sách mã IATA của các sân bay Việt Nam
vietnam_airports = [
    "HAN",  # Sân bay quốc tế Nội Bài
    "SGN",  # Sân bay quốc tế Tân Sơn Nhất
    "DAD",  # Sân bay quốc tế Đà Nẵng
    "CXR",  # Sân bay quốc tế Cam Ranh
    "PQC",  # Sân bay quốc tế Phú Quốc
    "HUI",  # Sân bay quốc tế Cát Bi
    "DLI",  # Sân bay quốc tế Liên Khương
    "THD",  # Sân bay Thọ Xuân
    "HUI",  # Sân bay Phú Bài
    "UIH",  # Sân bay Tuy Hòa
    "VDH",  # Sân bay Đồng Hới
    "VII",  # Sân bay Vinh
    "VCS",  # Sân bay Côn Đảo
    "VKG",  # Sân bay Rạch Giá
    "HPH"   # Sân bay Hải Phòng
]


def run_pipeline(**kwargs):
    """
    Get the flights JSON response. Transform its contents to CSV files regarding:
        - Best flights
        - Other flights
        - Search parameters
        - Price insights
    """
    trajectory = f'{kwargs["departure_id"]}-{kwargs["arrival_id"]}'
    search_date = datetime.now().strftime('%Y%m%d')
    outbound_date = kwargs['outbound_date'].replace('-', '')

    # Extract bronze data
    flight_details = {
        "engine": kwargs["engine"],
        "departure_id": kwargs["departure_id"],
        "arrival_id": kwargs["arrival_id"],
        "outbound_date": kwargs["outbound_date"],
        "currency": kwargs["currency"],
        "hl": kwargs["hl"]
    }

    # Nếu chuyến bay khứ hồi
    if 'return_date' in kwargs.keys():
        return_date = kwargs['return_date'].replace('-', '')
        branch_dir = f'{trajectory}/{outbound_date}-{return_date}/{search_date}'
        flight_details['return_date'] = kwargs['return_date']
    else:
        branch_dir = f'{trajectory}/{outbound_date}/{search_date}'

    # Lấy dữ liệu chuyến bay
    response = extract_flights_information(branch_dir, flight_details)

    # Tạo dữ liệu silver
    silver_output_dir = os.path.join(root_dir, f'data/output/{branch_dir}/silver')
    transform_to_csv(response, silver_output_dir)

    return {'statusCode': 200}


def get_all_flights_in_vietnam():
    """
    Gọi API để lấy toàn bộ các chuyến bay giữa các sân bay tại Việt Nam
    """
    # Ngày khởi hành (ví dụ), bạn có thể thay đổi thành ngày bạn cần
    outbound_date = '2024-10-05'
    return_date = '2024-10-19'

    # Lặp qua tất cả cặp sân bay
    for departure_id in vietnam_airports:
        for arrival_id in vietnam_airports:
            if departure_id != arrival_id:  # Bỏ qua chuyến bay cùng sân bay
                try:
                    # Gọi hàm run_pipeline cho mỗi cặp sân bay
                    run_pipeline(
                        engine='google_flights',  # Tên engine của API
                        departure_id=departure_id,  # Sân bay đi
                        arrival_id=arrival_id,  # Sân bay đến
                        outbound_date=outbound_date,  # Ngày khởi hành
                        return_date=return_date,
                        currency='VND',  # Đơn vị tiền tệ (VND)
                        hl='vi'  # Ngôn ngữ
                    )
                    print(f"Successfully fetched flights from {departure_id} to {arrival_id}")
                except Exception as e:
                    print(f"Error with flight from {departure_id} to {arrival_id}: {e}")


if __name__ == '__main__':
    get_all_flights_in_vietnam()  # Chạy hàm lấy tất cả chuyến bay giữa các sân bay VN

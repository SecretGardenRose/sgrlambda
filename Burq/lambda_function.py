import json
import boto3
import urllib3
from typing import Dict, List, Optional

class DeliveryService:
    def __init__(self, address_host: str, address_token: str):
        self.address_host = address_host  # API基础地址
        self.address_token = address_token  # API认证令牌

    # 创建配送信息
    def create_deliver(self, order_id: str, address: Optional[str] = None) -> bool:
        result = False

        http = urllib3.PoolManager()
        
        # TODO: 替换为实际数据库查询
        order_data = self._get_order_by_id(order_id)
        if not order_data:
            raise Exception("ORDER_NOT_FOUND!!!")

        # 检查报价是否有效
        if not self.query_quote(order_data):
            raise Exception("NOT_DELIVER_ORDER!!!")  # 不支持配送的订单
        print("query_quote success!!!")

        # 构造商品信息
        items = [{
            "name": order_data.get('productName', ''),
            "quantity": 1,
            "size": "small"
        }]

        # 拼接完整送货地址
        address_parts = []
        for field in ['address', 'address2', 'city', 'user_zip']:
            if order_data.get(field):
                address_parts.append(order_data[field])
        full_address = ', '.join(address_parts)
        
        # 组装配送参数
        delivery_params = {
            "external_order_ref": order_data.get('orderNumber', ''),
            "items": items,
            "dropoff_name": f"{order_data.get('firstName', '')} {order_data.get('lastName', '')}",
            "dropoff_address": full_address,
            "dropoff_phone_number": "(646)240-2703",
            "dropoff_notes": "double check address and deliver to person or call the phone number",
            "items_description": "flowers",
            "dropoff_unit": order_data.get('address2', ''),
            "initiate": False,
            "pickup_address": address if address else "120 west 28th street, New York, NY 10001",
            "pickup_phone_number": "6462402703",
            "pickup_name": "Secret Garden Rose",
            "pickup_notes": "pick up from unit 4L or call",
            "pickup_unit": "4L",
            "order_value": 15000
        }

        try:
            # 发送配送创建请求
            # 发送报价查询请求
            response = http.request(
                'POST',
                f"{self.address_host}v1/delivery_information",
                headers={"Content-Type": "application/json", "x-api-key": self.address_token},
                body=json.dumps(delivery_params)
            )

            # print(f"配送创建响应: {str(response.data)}")
            # print(f"配送创建响应: {str(response.status)}")

            if response.status in (200, 201):
                result = True
                result_data = json.loads(response.data.decode('utf-8'))
                # TODO: 更新订单配送ID到数据库
                order_data['deliveryId'] = result_data.get('id', '')
                self._update_order(order_data)
            else:
                print(f"配送创建失败: {response.text}")
        except Exception as e:
            print(f"创建配送时出错: {str(e)}")

        return result



    # 处理订单quote信息
    def query_quote(self, order_info: Dict) -> bool:
        # 如果有外部订单ID，则调用专用方法查询
        if order_info.get('externalId'):
            return self.query_quote_by_ext_order(order_info)

        result = False
        # 构造取货地址信息
        pickup_address = {
            "state": "NY",
            "country": "US",
            "street": "120 west 28th street",
            "city": "New York",
            "postal_code": "10001",
            "latitude": "40.740207",
            "longitude": "-73.998294"
        }

        # 检查配送地址的州是否有效
        if not order_info.get('state'):
            raise Exception("订单信息不完整!!!")
        state = self.check_state(order_info.get('state', ''))
        if not state:
            raise Exception("NOT_DELIVER_ORDER")  # 不支持的配送区域

        # 构造送货地址信息
        dropoff_address = {
            "state": state,
            "country": "US",
            "street": order_info.get('address', ''),
            "city": order_info.get('city', ''),
            "postal_code": order_info.get('userZip', ''),
            "latitude": order_info.get('latitude', ''),
            "longitude": order_info.get('longitude', '')
        }
        
        # 拼接完整送货地址字符串
        dropoff_address_main = f"{order_info.get('address', '')},{order_info.get('city', '')}," \
                             f"{order_info.get('state', '')},{order_info.get('user_zip', '')}"

        # 组装查询参数
        querys = {
            "pickup_address_details": pickup_address,
            "dropoff_address_details": dropoff_address,
            "pickup_address": "120 west 28th street, New York, NY, 10001",
            "dropoff_address": dropoff_address_main
        }

        # 设置请求头
        headers = {
            "Content-Type": "application/json",
            "x-api-key": self.address_token
        }

        try:
            http = urllib3.PoolManager()
            print("开始查询报价信息")
            # 发送报价查询请求
            response = http.request(
                'POST',
                f"{self.address_host}v1/quote",
                headers=headers,
                body=json.dumps(querys)
            )

            # 处理响应
            result_data = json.loads(response.data.decode('utf-8'))
            # print(f"响应结果: {result_data}")

            if response.status in (200, 201):
                for item in result_data:
                    # print(f"报价查询成功: {item.get("service")}")
                    # 检查服务提供商是否为uber
                    if item and item.get('service').lower() == 'uber':
                        result = True
                        break
            else:
                print(f"报价查询失败: {response.text}")
        except Exception as e:
            print(f"查询报价时发生错误: {str(e)}")

        return result
    

    # 查询外部订单信息
    def query_quote_by_ext_order(self, order_info: Dict) -> bool:
        result = False
        
       # 检查地址信息是否完整
        if not order_info.get('address'):
            raise Exception("NOT_DELIVER_ORDER")  # 地址信息不完整

        # 解析外部订单地址
        address_parts = order_info.get("address").split(',')
        if len(address_parts) != 4:
            raise Exception("NOT_DELIVER_ORDER2")  # 地址格式不正确

        state_zip = address_parts[2].strip().split()

        # 构造送货地址信息
        dropoff_address = {
            "state": state_zip[0],
            "country": "US",
            "street": address_parts[0],
            "city": address_parts[1].strip(),
            "postal_code": state_zip[1],
            "latitude": order_info.get('latitude', ''),
            "longitude": order_info.get('longitude', '')
        }

        # 组装查询参数
        querys = {
            "pickup_address_details": {
                "state": "NY",
                "country": "US",
                "street": "120 west 28th street",
                "city": "New York",
                "postal_code": "10001",
                "latitude": "40.740207",
                "longitude": "-73.998294"
            },
            "dropoff_address_details": dropoff_address,
            "pickup_address": "120 west 28th street, New York, NY, 10001",
            "dropoff_address": order_info.get("address")
        }

        try:
            http = urllib3.PoolManager()
            # 发送外部订单报价查询
            response = http.request(
                'POST',
                f"{self.address_host}v1/quote",
                headers={"Content-Type": "application/json", "x-api-key": self.address_token},
                body=json.dumps(querys)
            )

            # 处理响应
            result_data = json.loads(response.data.decode('utf-8'))
            # print(f"响应结果: {result_data}")

            if response.status in (200, 201):
                for item in result_data:
                    # print(f"报价查询成功: {item.get("service")}")
                    # 检查服务提供商是否为uber
                    if item and item.get('service').lower() == 'uber':
                        result = True
                        break
            else:
                print(f"外部订单报价查询失败: {response.text}")
            

        except Exception as e:
            print(f"外部订单查询出错: {str(e)}")

        return result

    def check_state(self, state: str) -> Optional[str]:
        # 检查州名并返回缩写
        state_map = {
            "NY": "New York",
            "NJ": "New Jersey",
            "CT": "Connecticut"
        }
        for code, name in state_map.items():
            if state.lower() in name.lower():
                return code
        return None
    

    # 数据查询
    def _get_order_by_id(self, order_id: str) -> Dict:
        """通过API查询订单信息"""
        # 创建 HTTP 客户端
        # 或者使用 urllib3（Lambda Python 环境内置）
        try:
            http = urllib3.PoolManager()
            
            # 你的API地址
            api_url = f"https://www.southiu.cn/south-fast/mall/mallorder/info/{order_id}"
            
            # 发送请求
            response = http.request(
                'GET',
                api_url,
                # fields=params,
                headers={'Content-Type': 'application/json'}
            )

            
            # 处理响应
            result = json.loads(response.data.decode('utf-8'))

            print(f"响应结果: {result.get("mallOrder")}")
            
            if response.status == 200 and result.get("code") == 0:
                print("订单信息查询成功！！！")
                return result.get("mallOrder")
            else:
                return None
                
        except Exception as e:
            print(f"查询订单信息失败：{str(e)}")
            return None

    # 订单数据更新
    def _update_order(self, order_data: Dict) -> bool:
        """TODO: 实现订单更新"""
        try:
            http = urllib3.PoolManager()
            
            # 你的API地址
            api_url = f"https://www.southiu.cn/south-fast/mall/mallorder/updateByDeliver"
            
            params = {
                "id":order_data.get("id"),
                "deliveryId":order_data.get("deliveryId")
            }

            # 发送请求
            response = http.request(
                'POST',
                api_url,
                body=json.dumps(params),
                headers={'Content-Type': 'application/json'}
            )

            
            # 处理响应
            result = json.loads(response.data.decode('utf-8'))

            
            if response.status == 200 and result.get("code") == 0:
                print("订单信息更新成功！！！")
                return True
            else:
                return False
                
        except Exception as e:
            print(f"更新订单信息失败：{str(e)}")
            return False
  
def lambda_handler(event, context):
    print(f"event:{str(event)}")
    
    print(f"queryStringParameters:{str(event.get("queryStringParameters"))}")
    print(f"queryStringParameters:{str(event.get("queryStringParameters").get("orderId"))}")
    orderId = event.get("orderId") if  event.get("orderId") else event.get("queryStringParameters").get("orderId")
    address = event.get("address") if  event.get("address") else event.get("queryStringParameters").get("address")
    print(f"orderId:{str(orderId)}")
    print(f"address:{str(address)}")

    # burq api
    service = DeliveryService(
        address_host="https://api.burqup.com/",
        # address_token="9f9943f1-0b60-4c12-a798-50ec897f3aa3"
        address_token="f271a030-b9af-4341-8162-180c6f7694c7"
    )
    
    # 测试州名检查
    print(service.check_state("New York"))  # 应输出 "NY"
    
    # 测试创建配送 17520
    success = service.create_deliver(orderId, address)
    # success = service.create_deliver("17520", "NY")
    print(f"配送创建{'成功' if success else '失败'}")
    return {
        'statusCode': 200,
        'body': json.dumps({'success': success})
    }

    
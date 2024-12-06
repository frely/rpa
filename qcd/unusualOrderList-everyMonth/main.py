import os
import sys
import requests
import json
import pandas as pd
import pymysql.cursors
import time
import arrow
import logging


# 初始化表格列表
sale_order_number_list = []
sale_type_list = []
sale_state_list = []
sale_date_list = []
salesperson_list = []
channel_id_list = []
level1_department_id_list = []
level2_department_id_list = []
level3_department_id_list = []
performance_department_id_list = []
is_apportionment_list = []
partner_name_list = []
lv2_product_category_list = []
performance_list = []
is_refund_list = []
phone_list = []
abnormal_cause_list = []
order_data_list = []

## odoo配置
# 获取cookie
baseUrl = os.getenv("odoo_host")
data = {
    'jsonrpc': '2.0',
    'method': 'call',
    'params': {
        'db': os.getenv("odoo_db"),
        'login': os.getenv("odoo_username"),
        'password': os.getenv("odoo_userpasswd"),
    }
}
session_response = requests.post(f'{baseUrl}/web/session/authenticate', json=data)
if session_response.status_code != 200:
    print(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()), '请求失败，重试中')
    time.sleep(10)
    session_response = requests.post(f'{baseUrl}/web/session/authenticate', json=data)
    if session_response.status_code != 200:
        print(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()), '请求失败，重试中')
        time.sleep(10)
    sys.exit(1)
session_data = session_response.json()
if session_data['result'] and session_response.cookies['session_id']:
    session_id = session_response.cookies['session_id']
else:
    print(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()), '登录失败')
    sys.exit(1)

headers = {
    'Content-Type': 'application/json',
    'Cookie': f"session_id={session_id}",
}


## 云客配置
yk_token = os.getenv('yk_token')
yk_headers = {
    'Content-Type': 'application/json',
    'Cookie': yk_token,
}


def find_salesDetailReport(find_num, order_data_start, order_data_stop):
    # 销售明细报表查询字段
    body = {
    "id": 8,
    "jsonrpc": "2.0",
    "method": "call",
    "params": {
        "model": "sale.order.line.report",
        "method": "web_search_read",
        "args": [],
        "kwargs": {
        "specification": {
            "sale_order_number": {},
            "sale_type": {},
            "sale_state": {},
            "financial_reconciliation": {},
            "sale_date": {},
            "return_origin_sale_id": {
            "fields": {
                "display_name": {}
            }
            },
            "return_origin_sale_date": {},
            "salesperson": {
            "fields": {
                "display_name": {}
            }
            },
            "system_order_number": {},
            "channel_id": {
            "fields": {
                "display_name": {}
            }
            },
            "level1_department_id": {
            "fields": {
                "display_name": {}
            },
            "context": {
                "hierarchical_naming": False
            }
            },
            "level2_department_id": {
            "fields": {
                "display_name": {}
            },
            "context": {
                "hierarchical_naming": False
            }
            },
            "level3_department_id": {
            "fields": {
                "display_name": {}
            },
            "context": {
                "hierarchical_naming": False
            }
            },
            "level4_department_id": {
            "fields": {
                "display_name": {}
            },
            "context": {
                "hierarchical_naming": False
            }
            },
            "department_id": {
            "fields": {
                "display_name": {}
            },
            "context": {
                "hierarchical_naming": False
            }
            },
            "performance_department_id": {
            "fields": {
                "display_name": {}
            },
            "context": {
                "hierarchical_naming": False
            }
            },
            "country_code": {
            "fields": {
                "display_name": {}
            }
            },
            "email": {},
            "customer_points_acquisition_new": {
            "fields": {
                "display_name": {}
            }
            },
            "receive_name": {},
            "is_apportionment": {},
            "partner_name": {},
            "product_code": {},
            "lv1_product_category": {
            "fields": {
                "display_name": {}
            },
            "context": {
                "hierarchical_naming": False
            }
            },
            "lv2_product_category": {
            "fields": {
                "display_name": {}
            },
            "context": {
                "hierarchical_naming": False
            }
            },
            "lv3_product_category": {
            "fields": {
                "display_name": {}
            },
            "context": {
                "hierarchical_naming": False
            }
            },
            "lv4_product_category": {
            "fields": {
                "display_name": {}
            },
            "context": {
                "hierarchical_naming": False
            }
            },
            "product_name": {},
            "vsn_code_display": {},
            "imei1_code_display": {},
            "imei2_code_display": {},
            "meid_code_display": {},
            "route_id": {
            "fields": {
                "display_name": {}
            }
            },
            "customer_source": {
            "fields": {
                "display_name": {}
            }
            },
            "apportionment_ratio": {},
            "apportionment_users": {},
            "apportionment_standard_unit_price": {},
            "standard_unit_price": {},
            "cny_standard_unit_price": {},
            "quantity": {},
            "real_quantity": {},
            "apportionment_quantity": {},
            "unit_price": {},
            "cny_unit_price": {},
            "currency_id": {
            "fields": {
                "display_name": {}
            }
            },
            "cny_currency_rate": {},
            "tax_included_price": {},
            "cny_deal_amount": {},
            "performance": {},
            "commission_discount": {},
            "amount_total_order_discounted": {},
            "amount_rewarded": {},
            "rewarded_display": {},
            "order_include_promotion": {},
            "discount": {},
            "total_discount": {},
            "rebate_price": {},
            "pay_method_new": {},
            "pay_time": {},
            "collection_subject": {},
            "delivery_time": {},
            "express_order_number": {},
            "total_amount": {},
            "cny_total_amount": {},
            "is_refund": {},
            "total_refund_amount": {},
            "quantity_delivered": {},
            "remark": {},
            "performance_discounted_rate": {}
        },
        "offset": 0,
        "order": "",
        "limit": find_num,
        "context": {
            "lang": "zh_CN",
            "tz": "Etc/GMT-8",
            "uid": 13103,
            "allowed_company_ids": [
            12
            ],
            "bin_size": True,
            "params": {
            "action": 1315,
            "model": "sale.order.line.report",
            "view_type": "list",
            "cids": 12,
            "menu_id": 256
            },
            "hierarchical_naming": False,
            "current_company_id": 12
        },
        "count_limit": 10001,
        "domain": [
            "&",
            [
            "sale_date",
            ">=",
            order_data_start
            ],
            [
            "sale_date",
            "<=",
            order_data_stop
            ]
        ]
        }
    }
    }

    response = requests.post(f'{baseUrl}/web/dataset/call_kw/sale.order.line.report/web_search_read', headers=headers, data=json.dumps(body))
    if response.status_code != 200:
        print(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()), '请求失败，重试中')
        time.sleep(10)
        response = requests.post(f'{baseUrl}/web/dataset/call_kw/sale.order.line.report/web_search_read', headers=headers, data=json.dumps(body))
        if response.status_code != 200:
            print(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()), '请求失败，重试中')
            time.sleep(10)
            response = requests.post(f'{baseUrl}/web/dataset/call_kw/sale.order.line.report/web_search_read', headers=headers, data=json.dumps(body))
            if response.status_code != 200:
                print(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()), '请求失败')
                sys.exit(1)
    find_salesDetailReport_data = response.json()['result']['records']
    if len(find_salesDetailReport_data) == 0:
        print(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()), "请求数据失败")
        sys.exit(1)
        

    ## 英转中
    print(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()), "数据获取完成，开始英转中")
    # 销售单类型
    sale_type_dict = {
        #"agent_sale": "代理单",
        #"agent_sale_replacement": "代理单换货",
        #"agent_sale_return": "代理单退货",
        #"giveaway_sale": "赠品单",
        #"online_promote_sale": "特促单",
        "sale": "订单",
        #"sale_promotion": "订单促销",
        #"sale_replacement": "订单换货",
        #"sale_return": "订单退货",
        #"service_sale": "服务单",
        }
    # 单据状态
    sale_state_dict = {
        "partial_payment": "审批中",
        "sale": "已完成",
        "sent": "支付中",
    }
    # 是否分摊
    is_apportionment_dict = {
        "no": "否",
        "yes": "是",
    }
    # 是否退款
    is_refund_dict = {
        "no": "否",
        "yes": "是",
    }
    #
    filter_list = [
        "零售",
        "直营",
        "sale",
    ]

    for i in find_salesDetailReport_data:
        if i['level1_department_id']['display_name'] == "线下事业部" or i['level1_department_id']['display_name'] == "线上事业部":
            if i['channel_id']['display_name'] in filter_list and i['sale_type'] in filter_list:
                channel_id_list.append(i['channel_id']['display_name'])
                sale_type_list.append(sale_type_dict[i['sale_type']])
                try:
                    sale_order_number_list.append(i['sale_order_number'])
                except:
                    sale_order_number_list.append("无")
                try:
                    if i['sale_state'] in sale_state_dict:
                        sale_state_list.append(sale_state_dict[i['sale_state']])
                    else:
                        sale_state_list.append(i['sale_state'])
                except:
                    sale_state_list.append("无")
                try:
                    sale_date_list.append(i['sale_date'])
                except:
                    sale_date_list.append("无")
                try:
                    salesperson_list.append(i['salesperson']['display_name'])
                except:
                    salesperson_list.append("无")
                try:
                    level1_department_id_list.append(i['level1_department_id']['display_name'])
                except:
                    level1_department_id_list.append("无")
                try:
                    level2_department_id_list.append(i['level2_department_id']['display_name'])
                except:
                    level2_department_id_list.append("无")
                try:
                    level3_department_id_list.append(i['level3_department_id']['display_name'])
                except:
                    level3_department_id_list.append("无")
                try:
                    performance_department_id_list.append(i['performance_department_id']['display_name'])
                except:
                    performance_department_id_list.append("无")
                try:
                    if i['is_apportionment'] in is_apportionment_dict:
                        is_apportionment_list.append(is_apportionment_dict[i['is_apportionment']])
                    else:
                        is_apportionment_list.append(i['is_apportionment'])
                except:
                    is_apportionment_list.append("无")
                try:
                    partner_name_list.append(i['partner_name'])
                except:
                    partner_name_list.append("无")
                try:
                    lv2_product_category_list.append(i['lv2_product_category']['display_name'])
                except:
                    lv2_product_category_list.append("无")
                try:
                    performance_list.append(i['performance'])
                except:
                    performance_list.append("无")
                try:
                    if i['is_refund'] in is_refund_dict:
                        is_refund_list.append(is_refund_dict[i['is_refund']])
                    else:
                        is_refund_list.append(i['is_refund'])
                except:
                    is_refund_list.append("无")


def find_saleOrderNumber_id(sale_order_number, order_data_start, order_data_stop):
    # 销售订单查询
    body = {
    "id": 62,
    "jsonrpc": "2.0",
    "method": "call",
    "params": {
        "model": "sale.order",
        "method": "web_search_read",
        "args": [],
        "kwargs": {
        "specification": {
            "message_needaction": {},
            "currency_id": {
            "fields": {}
            },
            "name": {},
            "date_order": {},
            "system_code": {},
            "commitment_date": {},
            "expected_date": {},
            "partner_name": {},
            "user_id": {
            "fields": {
                "display_name": {}
            }
            },
            "department_id": {
            "fields": {
                "display_name": {}
            }
            },
            "apportionment_users": {
            "fields": {
                "display_name": {}
            }
            },
            "operations_employee_id": {
            "fields": {
                "display_name": {}
            }
            },
            "team_id": {
            "fields": {
                "display_name": {}
            }
            },
            "company_id": {
            "fields": {
                "display_name": {}
            }
            },
            "amount_untaxed": {},
            "amount_tax": {},
            "amount_total": {},
            "tag_ids": {
            "fields": {
                "display_name": {},
                "color": {}
            }
            },
            "warehouse_id": {
            "fields": {
                "display_name": {}
            }
            },
            "state": {},
            "billing_status": {},
            "effective_date": {},
            "delivery_status": {},
            "invoice_status": {},
            "amount_to_invoice": {},
            "client_order_ref": {},
            "validity_date": {},
            "is_all_pay": {},
            "create_uid": {
            "fields": {
                "display_name": {}
            }
            },
            "create_date": {},
            "is_apportionment": {}
        },
        "offset": 0,
        "order": "",
        "limit": 80,
        "context": {
            "lang": "zh_CN",
            "tz": "Etc/GMT-8",
            "uid": 13103,
            "allowed_company_ids": [
            12
            ],
            "bin_size": True,
            "create": False,
            "current_company_id": 12
        },
        "count_limit": 10001,
        "domain": [
            "&",
            "&",
            "&",
            "&",
            [
            "service_mark",
            "=",
            False
            ],
            [
            "return_mark",
            "=",
            False
            ],
            [
            "agent_mark",
            "=",
            False
            ],
            "|",
            [
            "state",
            "not in",
            [
                "draft",
                "cancel",
                "sent"
            ]
            ],
            "&",
            [
            "state",
            "=",
            "sent"
            ],
            [
            "is_all_pay",
            "=",
            True
            ],
            "&",
            "&",
            [
            "date_order",
            ">=",
            order_data_start
            ],
            [
            "date_order",
            "<=",
            order_data_stop
            ],
            "|",
            "|",
            [
            "name",
            "ilike",
            sale_order_number
            ],
            [
            "client_order_ref",
            "ilike",
            sale_order_number
            ],
            [
            "partner_id",
            "child_of",
            sale_order_number
            ]
        ]
        }
    }
    }

    response = requests.post(f'{baseUrl}/web/dataset/call_kw/sale.order/web_search_read', headers=headers, data=json.dumps(body))
    if response.status_code != 200:
        print(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()), '请求失败，重试中')
        time.sleep(10)
        response = requests.post(f'{baseUrl}/web/dataset/call_kw/sale.order/web_search_read', headers=headers, data=json.dumps(body))
        if response.status_code != 200:
            print(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()), '请求失败，重试中')
            time.sleep(10)
            if response.status_code != 200:
                print(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()), '请求失败')
                sys.exit(1)
    id = response.json()['result']['records'][0]['id']
    return id

def find_saleOrderNumber_phone(id):
    body = {
    "id": 63,
    "jsonrpc": "2.0",
    "method": "call",
    "params": {
        "model": "sale.order",
        "method": "web_read",
        "args": [
        [
            id
        ]
        ],
        "kwargs": {
        "context": {
            "lang": "zh_CN",
            "tz": "Etc/GMT-8",
            "uid": 13103,
            "allowed_company_ids": [
            12
            ],
            "bin_size": True,
            "create": False
        },
        "specification": {
            "locked": {},
            "authorized_transaction_ids": {},
            "billing_status": {},
            "print_user_id": {
            "fields": {}
            },
            "sync_points_info_tag": {},
            "return_state": {},
            "state": {},
            "return_state_display_compute": {},
            "partner_credit_warning": {},
            "dropship_picking_count": {},
            "delivery_count": {},
            "return_goods_count": {},
            "billing_lines_count": {},
            "reward_count": {},
            "pos_order_count": {},
            "invoice_count": {},
            "xiao_count": {},
            "mrp_production_count": {},
            "purchase_order_count": {},
            "name": {},
            "reconciliation_tag_type": {},
            "account_reconciliation_remark": {},
            "user_id": {
            "fields": {
                "display_name": {}
            }
            },
            "department_id": {
            "fields": {
                "display_name": {}
            }
            },
            "is_supplier_company": {},
            "allow_select_department_domain_char": {},
            "partner_id": {
            "fields": {},
            "context": {
                "res_partner_search_mode": "customer",
                "show_address": 1,
                "show_vat": True
            }
            },
            "grid": {},
            "grid_product_tmpl_id": {
            "fields": {}
            },
            "grid_update": {},
            "delivery_set": {},
            "is_all_service": {},
            "recompute_delivery_price": {},
            "system_code": {},
            "pricelist_ids": {},
            "create_uid": {
            "fields": {}
            },
            "privacy_block": {},
            "country_code": {
            "fields": {
                "display_name": {}
            }
            },
            "phone": {},
            "email": {},
            "partner_name": {},
            "sex": {},
            "age": {},
            "receive_name": {},
            "receive_phone": {},
            "hwd_order_number": {},
            "partner_invoice_id": {
            "fields": {},
            "context": {
                "default_type": "invoice",
                "show_address": False,
                "show_vat": False
            }
            },
            "partner_shipping_id": {
            "fields": {},
            "context": {
                "default_type": "delivery",
                "show_address": False,
                "show_vat": False
            }
            },
            "ups_bill_my_account": {},
            "partner_ups_carrier_account": {},
            "validity_date": {},
            "date_order": {},
            "has_active_pricelist": {},
            "show_update_pricelist": {},
            "pricelist_id": {
            "fields": {}
            },
            "company_id": {
            "fields": {
                "display_name": {}
            }
            },
            "currency_id": {
            "fields": {
                "display_name": {}
            }
            },
            "tax_country_id": {
            "fields": {}
            },
            "tax_calculation_rounding_method": {},
            "payment_term_id": {
            "fields": {}
            },
            "channel_id": {
            "fields": {
                "display_name": {}
            }
            },
            "allowed_customer_sources": {},
            "customer_source": {
            "fields": {
                "display_name": {}
            }
            },
            "is_apportionment": {},
            "win_win_new": {
            "fields": {}
            },
            "customer_points_acquisition_new": {
            "fields": {
                "display_name": {}
            }
            },
            "point_sign": {},
            "mto_id": {},
            "delivery_method": {},
            "receive_address": {},
            "remark": {},
            "department_name": {},
            "department_phone": {},
            "order_line": {
            "fields": {
                "sequence": {},
                "display_type": {},
                "product_uom_category_id": {
                "fields": {}
                },
                "product_type": {},
                "product_updatable": {},
                "is_downpayment": {},
                "product_id": {
                "fields": {
                    "display_name": {}
                }
                },
                "product_template_id": {
                "fields": {
                    "display_name": {}
                }
                },
                "product_add_mode": {},
                "product_template_attribute_value_ids": {},
                "product_custom_attribute_value_ids": {},
                "product_no_variant_attribute_value_ids": {},
                "is_configurable_product": {},
                "categ_id": {
                "fields": {
                    "display_name": {}
                }
                },
                "mark_amount": {},
                "product_oss_photo": {},
                "name": {},
                "route_id": {
                "fields": {
                    "display_name": {}
                }
                },
                "order_id": {
                "fields": {}
                },
                "warehouse_ids": {},
                "line_route_warehouse_domain_char": {},
                "vsn_ids": {
                "fields": {
                    "display_name": {}
                }
                },
                "product_uom_qty": {},
                "vsn_id": {},
                "reward_id": {
                "fields": {}
                },
                "hwb_number": {},
                "is_vsn": {},
                "qty_delivered": {},
                "virtual_available_at_date": {},
                "qty_available_today": {},
                "free_qty_today": {},
                "scheduled_date": {},
                "forecast_expected_date": {},
                "warehouse_id": {
                "fields": {}
                },
                "move_ids": {},
                "qty_to_deliver": {},
                "is_mto": {},
                "display_qty_widget": {},
                "qty_delivered_method": {},
                "qty_invoiced": {},
                "qty_to_invoice": {},
                "product_uom_readonly": {},
                "product_uom": {
                "fields": {
                    "display_name": {}
                }
                },
                "customer_lead": {},
                "standard_unit_price": {},
                "recompute_delivery_price": {},
                "is_delivery": {},
                "price_unit": {},
                "tax_id": {
                "fields": {
                    "display_name": {}
                },
                "context": {
                    "active_test": True
                }
                },
                "price_total": {},
                "discount": {},
                "price_subtotal": {},
                "apply_gift": {},
                "amount_order_discounted": {},
                "amount_rewarded": {},
                "remark": {},
                "return_num": {},
                "user_id": {
                "fields": {}
                },
                "tax_calculation_rounding_method": {},
                "state": {},
                "invoice_status": {},
                "currency_id": {
                "fields": {}
                },
                "price_tax": {},
                "company_id": {
                "fields": {}
                }
            },
            "limit": 200,
            "order": "sequence ASC"
            },
            "note": {},
            "tax_totals": {},
            "amount_total": {},
            "no_pay_total": {},
            "sale_order_option_ids": {
            "fields": {
                "sequence": {},
                "product_id": {
                "fields": {
                    "display_name": {}
                }
                },
                "name": {},
                "quantity": {},
                "uom_id": {
                "fields": {
                    "display_name": {}
                }
                },
                "product_uom_category_id": {
                "fields": {}
                },
                "price_unit": {},
                "discount": {},
                "is_present": {}
            },
            "limit": 40,
            "order": "sequence ASC"
            },
            "team_id": {
            "fields": {
                "display_name": {}
            },
            "context": {
                "kanban_view_ref": "sales_team.crm_team_view_kanban"
            }
            },
            "require_signature": {},
            "require_payment": {},
            "prepayment_percent": {},
            "reference": {},
            "client_order_ref": {},
            "tag_ids": {
            "fields": {
                "display_name": {},
                "color": {}
            }
            },
            "show_update_fpos": {},
            "fiscal_position_id": {
            "fields": {
                "display_name": {}
            }
            },
            "invoice_status": {},
            "warehouse_id": {
            "fields": {
                "display_name": {}
            }
            },
            "incoterm": {
            "fields": {}
            },
            "incoterm_location": {},
            "picking_policy": {},
            "shipping_weight": {},
            "commitment_date": {},
            "expected_date": {},
            "show_json_popover": {},
            "json_popover": {},
            "effective_date": {},
            "delivery_status": {},
            "origin": {},
            "opportunity_id": {
            "fields": {
                "display_name": {}
            },
            "context": {
                "default_type": "opportunity"
            }
            },
            "campaign_id": {
            "fields": {
                "display_name": {}
            }
            },
            "medium_id": {
            "fields": {
                "display_name": {}
            }
            },
            "source_id": {
            "fields": {
                "display_name": {}
            }
            },
            "sale_order_payment_lines": {
            "fields": {
                "name": {},
                "pay_method": {},
                "pay_method_new": {
                "fields": {
                    "display_name": {}
                }
                },
                "pay_method_account_number": {
                "fields": {
                    "display_name": {}
                }
                },
                "pay_total": {},
                "pay_time": {},
                "attachment_ids": {
                "fields": {
                    "name": {},
                    "mimetype": {}
                }
                },
                "collection_subject": {
                "fields": {
                    "display_name": {}
                }
                },
                "collection_channel": {},
                "pay_user": {},
                "order_state": {},
                "state": {},
                "sale_order_name": {},
                "return_state": {},
                "return_mark": {}
            },
            "limit": 40,
            "order": "",
            "context": {
                "tree_view_ref": "vt_sale_order.sale_order_pay_line_tree"
            }
            },
            "all_pay_total": {},
            "is_all_pay": {},
            "apportionment_line": {
            "fields": {
                "user_id": {
                "fields": {
                    "display_name": {}
                }
                },
                "apportionment_ratio": {},
                "amount": {},
                "currency_id": {
                "fields": {}
                }
            },
            "limit": 40,
            "order": ""
            },
            "x_approval_status": {},
            "display_name": {}
        }
        }
    }
    }

    response = requests.post(f'{baseUrl}/web/dataset/call_kw/sale.order/web_read', headers=headers, data=json.dumps(body))
    if response.status_code != 200:
        print(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()), '请求失败，重试中')
        time.sleep(10)
        response = requests.post(f'{baseUrl}/web/dataset/call_kw/sale.order/web_read', headers=headers, data=json.dumps(body))
        if response.status_code != 200:
            print(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()), '请求失败，重试中')
            time.sleep(10)
            response = requests.post(f'{baseUrl}/web/dataset/call_kw/sale.order/web_read', headers=headers, data=json.dumps(body))
            if response.status_code != 200:
                print(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()), '请求失败')
                sys.exit(1)
    # 客户电话
    phone = response.json()['result'][0]['phone']
    return phone


def find_customerMsg_sql(find_customer_wechetId, time_range_start, time_range_stop):
    """
    SELECT `客户微信ID` FROM `云客聊天记录` WHERE `客户微信ID`=%s AND `消息时间` BETWEEN %s AND %s
    """
    connection = pymysql.connect(host=os.getenv('rpa_host'),
                                port=int(os.getenv('rpa_port')),
                                user=os.getenv('rpa_user'),
                                password=os.getenv('rpa_passwd'),
                                database=os.getenv('rpa_db'),
                                charset='utf8mb4',
                                cursorclass=pymysql.cursors.DictCursor)
    with connection:
        with connection.cursor() as cursor:
            sql = "SELECT `客户微信ID` FROM `云客聊天记录` WHERE `客户微信ID`=%s AND `消息时间` BETWEEN %s AND %s"
            cursor.execute(sql, (find_customer_wechetId, time_range_start, time_range_stop))
            return cursor.fetchall()
            

def run():
    print(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()), "执行中,请等待...")
    # 设置最大查询数: 默认10000
    find_num = 10000

    # 查询订单月份
    order_data = "2024-11"
    order_data_start = "2024-11-01"
    order_data_stop = "2024-11-30"

    # 获取订单中的客户电话
    find_salesDetailReport(find_num, order_data_start, order_data_stop)
    for i in sale_order_number_list:
        try:
            id = find_saleOrderNumber_id(i, order_data_start, order_data_stop)
        except:
            phone_list.append("")
            continue
        phone = find_saleOrderNumber_phone(id)
        phone_list.append(phone)

    df = pd.DataFrame(
        {"销售单号": sale_order_number_list,
        "销售单类型": sale_type_list,
        "单据状态": sale_state_list,
        "销售日期": sale_date_list,
        "销售员": salesperson_list,
        "渠道": channel_id_list,
        "一级部门": level1_department_id_list,
        "二级部门": level2_department_id_list,
        "三级部门": level3_department_id_list,
        "业绩所属门店": performance_department_id_list,
        "是否分摊": is_apportionment_list,
        "客户名称": partner_name_list,
        "客户电话": phone_list,
        "二级类别": lv2_product_category_list,
        "实际业绩": performance_list,
        "是否退款": is_refund_list,
        },
    )


    ## 排除正常订单
    num = 0
    drop_list = []
    msg_list = []

    

    for sale_order_number, sale_date, salesperson, level1_department_id, phone in zip(sale_order_number_list, sale_date_list, salesperson_list, level1_department_id_list, phone_list):
        if phone == "":
            pass
        """
        查询销售订单中的客户微信
        """
        if level1_department_id == "线下事业部":
            print(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()), "当前处理订单:", sale_order_number, level1_department_id, salesperson)

            # 查询销售订单中的客户电话与销售人员是否对应
            departmentId = "9A48CB9E3C76414DAAA60952B9DE4B0C" # 全部云客部门
            friendRemark = phone # 客户手机号
            realName = salesperson # 员工名称
            url = f"http://yk.vertuonline.com/pc/wechatcount/friendsList?departmentId={departmentId}&friendRemark={friendRemark}&realName={realName}"
            res = requests.post(url, headers=yk_headers)
            if res.status_code != 200:
                print(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()), '云客请求失败，重试中')
                time.sleep(10)
                res = requests.post(url, headers=yk_headers)
                if res.status_code != 200:
                    print(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()), '云客请求失败，重试中')
                    time.sleep(10)
                    res = requests.post(url, headers=yk_headers)
            if len(res.json()['data']['overviews']) == 0:
                abnormal_cause_list.append("无匹配的销售人员和云客客户备注")
                print("无匹配的销售人员和云客客户备注", friendRemark)
            else:
                friendId = res.json()['data']['overviews'][0]['friendId']
                # 判断下单时间3天内是否存在聊天记录
                time_range_start = arrow.get(sale_date + "T00:00:00.000000+08:00").shift(days=-2).format('YYYY-MM-DD HH:mm:ss')
                time_range_stop = arrow.get(sale_date + "T23:59:59.999999+08:00").format('YYYY-MM-DD HH:mm:ss')
                msg_list = find_customerMsg_sql(friendId, time_range_start, time_range_stop)
                if len(msg_list) != 0:
                    print("存在聊天记录")
                    abnormal_cause_list.append("存在聊天记录")
                    drop_list.append(num)
                else:
                    abnormal_cause_list.append("销售订单3天内无云客聊天记录")
                    print("销售订单3天内无云客聊天记录", friendRemark, sale_date)

        if level1_department_id == "线上事业部":
            print(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()), "当前处理订单:", sale_order_number, level1_department_id, salesperson)

            # 判断销售订单中的客户电话是否存在于云客客户表中
            departmentId = "14D229166ABD4A2E9D7DC62F0903EA3A" # 线上事业部
            friendRemark = phone # 客户手机号
            url = f"http://yk.vertuonline.com/pc/wechatcount/friendsList?departmentId={departmentId}&friendRemark={friendRemark}"
            res = requests.post(url, headers=yk_headers)
            if res.status_code != 200:
                print(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()), '云客请求失败，重试中')
                time.sleep(10)
                res = requests.post(url, headers=yk_headers)
                if res.status_code != 200:
                    print(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()), '云客请求失败，重试中')
                    time.sleep(10)
                    res = requests.post(url, headers=yk_headers)
            if len(res.json()['data']['overviews']) == 0:
                abnormal_cause_list.append("无匹配的云客客户备注")
                print("无匹配的云客客户备注", friendRemark)
            else:
                friendId = res.json()['data']['overviews'][0]['friendId']
                # 判断下单时间3天内是否存在聊天记录
                time_range_start = arrow.get(sale_date + "T00:00:00.000000+08:00").shift(days=-2).format('YYYY-MM-DD HH:mm:ss')
                time_range_stop = arrow.get(sale_date + "T23:59:59.999999+08:00").format('YYYY-MM-DD HH:mm:ss')
                msg_list = find_customerMsg_sql(friendId, time_range_start, time_range_stop)
                if len(msg_list) != 0:
                    print("存在聊天记录")
                    abnormal_cause_list.append("存在聊天记录")
                    drop_list.append(num)
                else:
                    abnormal_cause_list.append("销售订单3天内无云客聊天记录")
                    print("销售订单3天内无云客聊天记录", friendRemark, sale_date)
        num += 1
    
    if len(abnormal_cause_list) == 0:
        df["异常备注"] = ""
    else:
        df["异常备注"] = abnormal_cause_list
        # 删除正常的数据
        df = df.drop(drop_list)
    df["月份"] = order_data

    df.to_excel('销售明细报表异常订单.xlsx', index=False)
    print(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()), "程序执行完毕")

if __name__ == '__main__':
    run()
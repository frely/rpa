import os
import requests
import json
import pandas as pd

baseUrl = 'https://admin.vdaoai.com'

# 创建一个 Session 对象
s = requests.Session()

# 登录网站
# payload = {'username': 'xiao.wang@vdao365.com', 'password': 'gS6@wY2#lI1=eB1'}
# headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
#         'Content-Type': 'application/x-www-form-urlencoded'}
# s.post(baseurl + '/web/login', data=payload, headers=headers)

cookie = os.getenv('odoo-cookie')
headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
        'Content-Type': 'application/json',
        'Cookie': cookie}

def find_salesDetailReport():
    # 销售明细报表查询字段
    body = {
    "id": 12,
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
                "hierarchical_naming": 'false'
            }
            },
            "level2_department_id": {
            "fields": {
                "display_name": {}
            },
            "context": {
                "hierarchical_naming": 'false'
            }
            },
            "level3_department_id": {
            "fields": {
                "display_name": {}
            },
            "context": {
                "hierarchical_naming": 'false'
            }
            },
            "level4_department_id": {
            "fields": {
                "display_name": {}
            },
            "context": {
                "hierarchical_naming": 'false'
            }
            },
            "department_id": {
            "fields": {
                "display_name": {}
            },
            "context": {
                "hierarchical_naming": 'false'
            }
            },
            "performance_department_id": {
            "fields": {
                "display_name": {}
            },
            "context": {
                "hierarchical_naming": 'false'
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
                "hierarchical_naming": 'false'
            }
            },
            "lv2_product_category": {
            "fields": {
                "display_name": {}
            },
            "context": {
                "hierarchical_naming": 'false'
            }
            },
            "lv3_product_category": {
            "fields": {
                "display_name": {}
            },
            "context": {
                "hierarchical_naming": 'false'
            }
            },
            "lv4_product_category": {
            "fields": {
                "display_name": {}
            },
            "context": {
                "hierarchical_naming": 'false'
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
        "limit": 3,
        "context": {
            "lang": "zh_CN",
            "tz": "Etc/GMT-8",
            "uid": 13103,
            "allowed_company_ids": [
            12
            ],
            "bin_size": 'true',
            "hierarchical_naming": 'false',
            "current_company_id": 12
        },
        "count_limit": 10001,
        "domain": [
            "&",
            [
            "sale_date",
            ">=",
            "2024-10-01"
            ],
            [
            "sale_date",
            "<=",
            "2024-10-31"
            ]
        ]
        }
    }
    }

    response = s.post(f'{baseUrl}/web/dataset/call_kw/sale.order.line.report/web_search_read', headers=headers, data=json.dumps(body))
    find_salesDetailReport_data = response.json()['result']['records']

    # 初始化列表
    sale_order_number = []
    sale_type = []
    sale_state = []
    sale_date = []
    salesperson = []
    channel_id = []
    level1_department_id = []
    level2_department_id = []
    level3_department_id = []
    performance_department_id = []
    is_apportionment = []
    partner_name = []
    lv2_product_category = []
    performance = []
    is_refund = []
    for i in find_salesDetailReport_data:
        sale_order_number.append(i['sale_order_number'])
        sale_type.append(i['sale_type'])
        sale_state.append(i['sale_state'])
        sale_date.append(i['sale_date'])
        salesperson.append(i['salesperson']['display_name'])
        channel_id.append(i['channel_id']['display_name'])
        level1_department_id.append(i['level1_department_id']['display_name'])
        level2_department_id.append(i['level2_department_id']['display_name'])
        level3_department_id.append(i['level3_department_id']['display_name'])
        performance_department_id.append(i['performance_department_id']['display_name'])
        is_apportionment.append(i['is_apportionment'])
        partner_name.append(i['partner_name'])
        lv2_product_category.append(i['lv2_product_category']['display_name'])
        performance.append(i['performance'])
        is_refund.append(i['is_refund'])

    df = pd.DataFrame(
        {"销售单号": sale_order_number,
         "销售单类型": sale_type,
         "单据状态": sale_state,
         "销售日期": sale_date,
         "销售员": salesperson,
         "渠道": channel_id,
         "一级部门": level1_department_id,
         "二级部门": level2_department_id,
         "三级部门": level3_department_id,
         "业绩所属门店": performance_department_id,
         "是否分摊": is_apportionment,
         "客户名称": partner_name,
         #"客户电话": [4, 5, 6],
         "二级类别": lv2_product_category,
         "实际业绩": performance,
         "是否退款": is_refund,
        },
        #index=["x", "y", "z"]
    )
    #print(df)
    #print(sale_order_number, sale_type, sale_state, sale_date, salesperson, channel_id)
    df.to_excel('销售明细报表.xlsx', index=False)


def find_saleOrderNumber_id(sale_order_number):
    #sale_order_number = "XSD24103116017"
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
            "2024-10-01"
            ],
            [
            "date_order",
            "<=",
            "2024-10-31"
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

    response = s.post(f'{baseUrl}/web/dataset/call_kw/sale.order/web_search_read', headers=headers, data=json.dumps(body))
    id = response.json()['result']['records'][0]['id']
    print(id)

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

    response = s.post(f'{baseUrl}/web/dataset/call_kw/sale.order/web_read', headers=headers, data=json.dumps(body))
    # 客户电话
    phone = response.json()['result'][0]['phone']
    print(phone)


if __name__ == '__main__':
    find_salesDetailReport()
    #find_saleOrderNumber_id("XSD24103116017")
    #find_saleOrderNumber_phone(137088)
# Widget Factory Data Schemas
# Defines the structure of all messages in the manufacturing system

from datetime import datetime
from typing import Dict, List, Optional
from enum import Enum

class ComponentType(Enum):
    METAL_FRAME = "metal_frame"
    PLASTIC_HOUSING = "plastic_housing"
    CIRCUIT_BOARD = "circuit_board"
    BATTERY = "battery"
    DISPLAY_SCREEN = "display_screen"
    SENSOR = "sensor"
    MOTOR = "motor"

class WidgetType(Enum):
    BASIC_WIDGET = "basic_widget"
    PREMIUM_WIDGET = "premium_widget"
    INDUSTRIAL_WIDGET = "industrial_widget"
    SMART_WIDGET = "smart_widget"

class MachineStatus(Enum):
    IDLE = "idle"
    RUNNING = "running"
    MAINTENANCE = "maintenance"
    ERROR = "error"
    SHUTDOWN = "shutdown"

class QualityStatus(Enum):
    PASS = "pass"
    FAIL = "fail"
    PENDING = "pending"

# Widget Recipes - what components make each widget type
WIDGET_RECIPES = {
    WidgetType.BASIC_WIDGET: [
        ComponentType.METAL_FRAME,
        ComponentType.PLASTIC_HOUSING,
        ComponentType.CIRCUIT_BOARD
    ],
    WidgetType.PREMIUM_WIDGET: [
        ComponentType.METAL_FRAME,
        ComponentType.PLASTIC_HOUSING,
        ComponentType.CIRCUIT_BOARD,
        ComponentType.BATTERY,
        ComponentType.DISPLAY_SCREEN
    ],
    WidgetType.INDUSTRIAL_WIDGET: [
        ComponentType.METAL_FRAME,
        ComponentType.PLASTIC_HOUSING,
        ComponentType.CIRCUIT_BOARD,
        ComponentType.BATTERY,
        ComponentType.SENSOR,
        ComponentType.MOTOR
    ],
    WidgetType.SMART_WIDGET: [
        ComponentType.METAL_FRAME,
        ComponentType.PLASTIC_HOUSING,
        ComponentType.CIRCUIT_BOARD,
        ComponentType.BATTERY,
        ComponentType.DISPLAY_SCREEN,
        ComponentType.SENSOR
    ]
}

# Message Schemas
COMPONENT_PRODUCED_SCHEMA = {
    "component_id": str,
    "component_type": str,  # ComponentType
    "machine_id": str,
    "batch_id": str,
    "produced_at": str,
    "quality_metrics": {
        "weight_grams": float,
        "dimensions_mm": {"length": float, "width": float, "height": float},
        "temperature_celsius": float,
        "defect_count": int
    },
    "production_line": str
}

WIDGET_ASSEMBLY_SCHEMA = {
    "widget_id": str,
    "widget_type": str,  # WidgetType
    "assembly_station_id": str,
    "component_ids": [str],
    "assembly_started_at": str,
    "assembly_completed_at": str,
    "assembly_duration_seconds": float,
    "worker_id": str,
    "batch_id": str
}

QUALITY_CHECK_SCHEMA = {
    "check_id": str,
    "widget_id": str,
    "inspector_id": str,
    "checked_at": str,
    "status": str,  # QualityStatus
    "test_results": {
        "electrical_test": bool,
        "mechanical_test": bool,
        "visual_inspection": bool,
        "performance_score": float  # 0-100
    },
    "defects_found": [str],
    "notes": str
}

MACHINE_TELEMETRY_SCHEMA = {
    "machine_id": str,
    "machine_type": str,
    "timestamp": str,
    "status": str,  # MachineStatus
    "metrics": {
        "temperature_celsius": float,
        "vibration_level": float,
        "power_consumption_watts": float,
        "cycle_count": int,
        "efficiency_percent": float
    },
    "location": {
        "building": str,
        "floor": int,
        "line": str
    }
}

INVENTORY_UPDATE_SCHEMA = {
    "item_type": str,  # "component" or "widget"
    "item_id": str,
    "warehouse_location": str,
    "quantity_change": int,  # positive for additions, negative for usage
    "current_stock": int,
    "updated_at": str,
    "reason": str,  # "production", "shipment", "quality_reject", etc.
    "batch_id": str
}
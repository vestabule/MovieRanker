
from django.urls import path
from .views import SignUpView, dashboard

urlpatterns = [
    path("signup/", SignUpView.as_view(), name="signup"),
    path("dashboard/", dashboard, name="dashboard"),
]

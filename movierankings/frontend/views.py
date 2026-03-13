from django.shortcuts import render

# Create your views here.

def home(request):
    return render(request, "home.html")

def demo(request):
    return render(request, "demo_test.html")

def signup(request):
    return render(request, "signup.html")
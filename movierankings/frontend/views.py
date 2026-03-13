from django.shortcuts import render

# Create your views here.

def home(request):
    return render(request, "home.html")

def demo(request):
    return render(request, "demo_test.html")

def signup(request):
    return render(request, "signup.html")

def movies(request):
    return render(request, "search.html")

def movie_details(request):
    
    id = None
    if request.method == "GET":
        id = request.GET.get("id")
    
    return render(request, "movie_details.html", {"id": id})

def ratings(request):
    return render(request, "ratings.html")
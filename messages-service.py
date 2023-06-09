from fastapi import FastAPI

app = FastAPI()

@app.get('/messages')
def static_message():
    return "Not implemented yet"

if __name__ == '__main__':
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=25002)
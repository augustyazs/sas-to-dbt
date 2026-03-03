from openai import OpenAI

client = OpenAI(api_key="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")

response = client.responses.create(
    model="gpt-5",
    input="Say hello"
)

print(response.output_text)

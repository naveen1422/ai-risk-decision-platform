from openai import OpenAI
import os

client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

def generate_llm_explanation(customer, similar_docs, decision):
    
    prompt = f"""
You are a risk analysis AI assistant.

Customer Details:
- Customer ID: {customer['customer_id']}
- Risk Score: {customer['risk_score']}
- Risk Bucket: {customer['risk_bucket']}
- Flags: {customer['risk_flags']}

Similar Customers:
{similar_docs}

Final Decision:
{decision}

Explain in simple business language:
- Why this decision was made
- What patterns were observed
- Keep it professional and concise
"""

    response = client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[{"role": "user", "content": prompt}],
        temperature=0.3
    )

    return response.choices[0].message.content
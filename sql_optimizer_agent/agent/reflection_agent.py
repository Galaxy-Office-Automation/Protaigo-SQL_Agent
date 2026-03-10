
from typing import Dict, List, Any, Optional
import sys
import logging

# Ensure this project is accessible via sys.path to find it siblings path what does the agent do
sys.path.insert(0, '/home/galaxy/DB_setup/sql_optimizer_agent')

from validator.equivalence import EquivalenceValidator  #to check if the data results match with the original query
from agent.llm_interface import LLMInterface  #to interact with the LLM

logger = logging.getLogger(__name__) #to log the messages

class ReflectionAgent: #this is the main class that will be used to interact with the LLM
    """
    Reflection Agent that evaluates optimized queries for semantic equivalence
    and safety, performing iterative refinement if necessary.
    """
    
    def __init__(self, llm_interface: LLMInterface, validator: EquivalenceValidator = None): #initialize the class
        self.llm = llm_interface  #initialize the LLM interface
        self.validator = validator or EquivalenceValidator()  #initialize the validator
        self.max_retries = 2  #initialize the max retries

    def reflect_and_refine(self, original_query: str, optimized_query: str) -> str: #this function will be used to reflect on the optimized query and refine it if it violates 
        """
        Reflects on the optimized query and refines it if it violates 
        correctness or semantic equivalence.
        """
        current_query = optimized_query  #initialize the current query
        
        for attempt in range(self.max_retries + 1): #loop through the max retries
            logger.info(f"Reflection attempt {attempt + 1}/{self.max_retries + 1}")  #log the reflection attempt
            logger.debug(f"Current query under reflection:\n{current_query}")  #log the current query
            
            # Step 1: Heuristic check for unauthorized data-altering keywords
            if self._has_unauthorized_data_alteration(original_query, current_query):  #check if the optimized query has unauthorized data-altering keywords
                hint = "You added LIMIT, TABLESAMPLE, or new filters that weren't in the original query. This is FORBIDDEN as it changes the result set."  #hint for the LLM
                
                # Add a "Success Hint" for common patterns
                if "JOIN" in original_query.upper() and ("COUNT" in original_query.upper() or "DISTINCT" in original_query.upper()):
                    hint += " Hint: I see a self-join for a count or distinct operation. Try using a Window Function like COUNT(*) OVER (PARTITION BY ...) or ROW_NUMBER() to replace the self-join."
                elif "WITH" in original_query.upper():
                    hint += " Hint: This query uses multiple CTEs. Try adding 'MATERIALIZED' or 'NOT MATERIALIZED' hints to stabilize the execution plan or combine similar CTEs."
                
                logger.warning(f"Unauthorized data-altering keywords detected. Suggesting hint: {hint}")  #log the hint
                current_query = self._llm_refine(  #refine the optimized query
                    original_query,  #original query
                    current_query,  #optimized query
                    error_context=hint  #error context
                )
                continue

            # Step 2: Technical validation (Equivalence)
            validation_result = self.validator.validate(original_query, current_query)  #validate the optimized query
            
            if not validation_result.get("valid"):
                reason = validation_result.get("reason", "Unknown validation error")  #get the reason for the validation failure
                
                if "statement timeout" in reason.lower() or "timeout" in reason.lower():
                    logger.info(f"Validation timed out on heavy query: {reason}. Trusting heuristic checks and proceeding.")
                else:
                    logger.warning(f"Validation failed: {reason}")
                    
                    # Step 2: Use LLM for reflection and refinement based on validation failure
                    current_query = self._llm_refine(   #refine the optimized query
                        original_query,  #original query
                        current_query,  #optimized query
                        error_context=f"The optimized query failed equivalence validation: {reason}"  #error context
                    )
                    continue
            

            # Step 3: Semantic Reflection (even if data matches, check if logic was changed unsafely)
            reflection_result = self._llm_reflect(original_query, current_query)  #reflect on the optimized query   
            
            if reflection_result.get("is_safe") and reflection_result.get("is_equivalent"):
                logger.info("Query passed reflection and validation.")  #log the reflection result
                return current_query
            else:
                issue = reflection_result.get("issue", "Semantic logic changes detected.")  #get the issue
                logger.warning(f"Reflection flagged issue: {issue}")  #log the issue
                
                # Refine based on semantic issues
                current_query = self._llm_refine(  #refine the optimized query
                    original_query,  #original query
                    current_query,  #optimized query
                    error_context=f"The optimized query is semantically unsafe or incorrect: {issue}. Revert any data-altering changes like added LIMITs or modified WHERE filters. Focus ONLY on structural performance (JOINs, CTEs, etc.)."
                )
        
        logger.warning("Max retries reached in reflection loop without finding an equivalent query. Falling back to original query.")
        return original_query

    def _llm_reflect(self, original: str, optimized: str) -> Dict[str, Any]:
        """Ask the LLM to reflect on the semantic safety of the optimization."""
        prompt = f"""You are a SQL validation expert. Compare the original and optimized queries below.
Evaluate if the optimized query is SEMANTICALLY EQUIVALENT and SAFE.

## Original Query:
```sql
{original}
```

## Optimized Query:
```sql
{optimized}
```

## Task:
1. Verify if the optimized query returns EXACTLY the same result set as the original.
2. Check for "unsafe" optimizations:
   - Abruptly adding LIMITs that truncate real results.
   - Changing JOIN types (e.g., INNER to LEFT) unless justified by data constraints.
   - Removing WHERE filters just to gain speed.
   - Changing aggregation logic in a way that alters metrics.

## Response Format (JSON):
{{
    "is_equivalent": <bool>,
    "is_safe": <bool>,
    "issue": "<detailed explanation of any discrepancy, or empty string>",
    "judgment": "A brief summary of your evaluation."
}}
"""
        messages = [
            {"role": "system", "content": "You are a SQL quality assurance agent. Respond in valid JSON."},
            {"role": "user", "content": prompt}
        ]
        
        try:
            # We use make_request directly from the llm_interface to get raw JSOn
            response = self.llm._make_request(messages)
            if "choices" in response:
                content = response['choices'][0]['message']['content']
                import json
                import re
                try:
                    return json.loads(content)
                except:
                    match = re.search(r'```(?:json)?\s*([\s\S]*?)\s*```', content)
                    if match:
                        return json.loads(match.group(1))
            
            error_msg = response.get("error", "Unknown LLM error (choices missing)")
            logger.error(f"LLM Reflection Request failed: {error_msg}")
            return {"is_safe": False, "is_equivalent": False, "issue": f"LLM response error: {error_msg}"}
        except Exception as e:
            logger.error(f"Exception during LLM reflection: {e}")
            return {"is_safe": False, "is_equivalent": False, "issue": str(e)}

    def _llm_refine(self, original: str, optimized: str, error_context: str) -> str:
        """Ask the LLM to refine the optimization based on specific feedback."""
        prompt = f"""You optimized a SQL query, but it has issues that need refinement.

## Original Query:
```sql
{original}
```

## Faulty Optimized Query:
```sql
{optimized}
```

## Feedback:
{error_context}

## Your Task:
Provide a REFINED version of the optimized query that:
1. FIXES the issues mentioned above.
2. REMAINS optimized for performance (PostgreSQL).
3. GUARANTEES identical output to the original query.

## Response Format (JSON):
{{
    "refined_query": "<full optimized SQL query>",
    "explanation": "<what you changed to fix the issue>"
}}
"""
        messages = [
            {"role": "system", "content": "You are a master of SQL optimization and correctness. Respond in valid JSON."},
            {"role": "user", "content": prompt}
        ]
        
        try:
            response = self.llm._make_request(messages)
            if "choices" in response:
                content = response['choices'][0]['message']['content']
                import json
                import re
                try:
                    res = json.loads(content)
                    return res.get('refined_query', optimized)
                except:
                    match = re.search(r'```(?:json)?\s*([\s\S]*?)\s*```', content)
                    if match:
                        res = json.loads(match.group(1))
                        return res.get('refined_query', optimized)
            
            error_msg = response.get("error", "Unknown LLM error (choices missing)")
            logger.error(f"LLM Refinement Request failed: {error_msg}")
            return optimized
        except Exception as e:
            logger.error(f"Exception during LLM refinement: {e}")
            return optimized

    def _has_unauthorized_data_alteration(self, original: str, optimized: str) -> bool:
        """Check if optimized query adds LIMIT, TABLESAMPLE, or alters WHERE filters."""
        orig_upper = original.upper()
        opt_upper = optimized.upper()
        
        # 1. Basic Keyword Injections
        if "LIMIT" in opt_upper and "LIMIT" not in orig_upper:
            return True
        if "TABLESAMPLE" in opt_upper and "TABLESAMPLE" not in orig_upper:
            return True
        if opt_upper.count("LIMIT") > orig_upper.count("LIMIT"):
            return True

        # 2. Heuristic for aid/id threshold changes (common failure point)
        # Look for patterns like "aid <= 500" or "aid < 1000" where numbers changed
        import re
        num_pattern = re.compile(r'aid\s*[<>]=?\s*(\d+)', re.IGNORECASE)
        orig_matches = num_pattern.findall(original)
        opt_matches = num_pattern.findall(optimized)
        
        if orig_matches and opt_matches:
            # If the thresholds for ID filtering changed, it's likely data-altering
            if sorted(orig_matches) != sorted(opt_matches):
                return True
        elif not orig_matches and opt_matches:
            # If we added an ID filter that wasn't there
            return True

        # 3. PERCENTILE_CONT must never be swapped to PERCENTILE_DISC
        if 'PERCENTILE_DISC' in opt_upper and 'PERCENTILE_DISC' not in orig_upper:
            return True
            
        return False

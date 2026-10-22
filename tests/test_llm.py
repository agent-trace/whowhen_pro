import unittest
from types import SimpleNamespace
from unittest.mock import patch

from whowhen_eval.llm import generate


class LlmGenerationTests(unittest.TestCase):
    @staticmethod
    def _response():
        return SimpleNamespace(
            choices=[
                SimpleNamespace(
                    message=SimpleNamespace(content=None),
                    finish_reason="length",
                )
            ],
            usage=SimpleNamespace(
                prompt_tokens=7,
                completion_tokens=11,
                total_tokens=18,
                completion_tokens_details=SimpleNamespace(reasoning_tokens=11),
            ),
        )

    def test_none_max_tokens_is_omitted_and_diagnostics_are_preserved(self):
        with patch("whowhen_eval.llm.litellm.completion", return_value=self._response()) as call:
            text, usage = generate("test/mock-model", [], max_tokens=None)

        self.assertEqual(text, "")
        self.assertNotIn("max_tokens", call.call_args.kwargs)
        self.assertEqual(usage["reasoning_tokens"], 11)
        self.assertEqual(usage["finish_reason"], "length")

    def test_explicit_max_tokens_is_forwarded(self):
        with patch("whowhen_eval.llm.litellm.completion", return_value=self._response()) as call:
            generate("test/mock-model", [], max_tokens=4096)

        self.assertEqual(call.call_args.kwargs["max_tokens"], 4096)


if __name__ == "__main__":
    unittest.main()
